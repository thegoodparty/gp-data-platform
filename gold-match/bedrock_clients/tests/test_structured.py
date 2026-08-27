from unittest.mock import patch

import boto3
import pytest
from botocore.config import Config
from botocore.stub import Stubber

from bedrock_clients.structured import (
    BedrockStructuredContentClient,
    StructuredOutputError,
    _schema_fingerprint,
)

MODEL_ID = "global.anthropic.claude-haiku-4-5-20251001-v1:0"

SCHEMA = {
    "type": "object",
    "properties": {
        "selected_candidate_number": {"type": "number", "minimum": 0, "maximum": 3},
        "selection_confidence": {"type": "number", "minimum": 0, "maximum": 100},
    },
    "required": ["selected_candidate_number", "selection_confidence"],
}


def make_client_and_stubber():
    client = boto3.client(
        "bedrock-runtime",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )
    return client, Stubber(client)


def expected_converse(prompt: str) -> dict:
    return {
        "modelId": MODEL_ID,
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "inferenceConfig": {"temperature": 0.0, "maxTokens": 2048},
        "toolConfig": {
            "tools": [
                {
                    "toolSpec": {
                        "name": "emit_match_selection",
                        "description": "Return the selection as arguments to this tool.",
                        "inputSchema": {"json": SCHEMA},
                    }
                }
            ],
            "toolChoice": {"tool": {"name": "emit_match_selection"}},
        },
    }


def converse_response(tool_input: dict | None, stop_reason: str = "tool_use") -> dict:
    content = [{"toolUse": {"toolUseId": "t1", "name": "emit_match_selection", "input": tool_input}}]
    if tool_input is None:
        content = [{"text": "I cannot use tools right now."}]
    return {
        "output": {"message": {"role": "assistant", "content": content}},
        "stopReason": stop_reason,
        "usage": {"inputTokens": 800, "outputTokens": 300, "totalTokens": 1100},
        "metrics": {"latencyMs": 100},
    }


def make_llm(stub_client) -> BedrockStructuredContentClient:
    return BedrockStructuredContentClient(bedrock_runtime=stub_client)


def test_happy_path_returns_tool_input_verbatim():
    client, stubber = make_client_and_stubber()
    payload = {"selected_candidate_number": 2, "selection_confidence": 90}
    stubber.add_response("converse", converse_response(payload), expected_converse("pick one"))
    with stubber:
        llm = make_llm(client)
        out = llm.generate_structured_content(
            prompt="pick one", response_schema=SCHEMA, trace_name="stitch-match-selection"
        )
    assert out == payload
    assert llm.get_usage_stats()["api_calls"] == 1


def test_output_shape_miss_gets_exactly_one_reask():
    client, stubber = make_client_and_stubber()
    good = {"selected_candidate_number": 1, "selection_confidence": 80}
    stubber.add_response("converse", converse_response(good, stop_reason="max_tokens"), expected_converse("p"))
    stubber.add_response("converse", converse_response(good), expected_converse("p"))
    with stubber:
        llm = make_llm(client)
        out = llm.generate_structured_content(prompt="p", response_schema=SCHEMA)
    assert out == good
    # Both responses were billable.
    assert llm.get_usage_stats()["api_calls"] == 2


def test_second_truncation_raises():
    client, stubber = make_client_and_stubber()
    good = {"selected_candidate_number": 1, "selection_confidence": 80}
    for _ in range(2):
        stubber.add_response("converse", converse_response(good, stop_reason="max_tokens"), expected_converse("p"))
    with stubber:
        llm = make_llm(client)
        with pytest.raises(StructuredOutputError, match="max_tokens"):
            llm.generate_structured_content(prompt="p", response_schema=SCHEMA)
    stubber.assert_no_pending_responses()


def test_no_tool_use_raises_after_reask():
    client, stubber = make_client_and_stubber()
    for _ in range(2):
        stubber.add_response("converse", converse_response(None, stop_reason="end_turn"), expected_converse("p"))
    with stubber:
        llm = make_llm(client)
        with pytest.raises(StructuredOutputError, match="stop"):
            llm.generate_structured_content(prompt="p", response_schema=SCHEMA)


def test_schema_violation_raises_and_usage_still_counted():
    client, stubber = make_client_and_stubber()
    bad = {"selected_candidate_number": 2, "selection_confidence": 950}
    for _ in range(2):
        stubber.add_response("converse", converse_response(bad), expected_converse("p"))
    with stubber:
        llm = make_llm(client)
        with pytest.raises(StructuredOutputError, match="schema"):
            llm.generate_structured_content(prompt="p", response_schema=SCHEMA)
    stats = llm.get_usage_stats()
    assert stats["api_calls"] == 2
    assert stats["prompt_tokens"] == 1600
    assert stats["total_cost"] > 0


def test_validation_failure_raises_inside_the_traced_callable():
    """The Braintrust span must see the failure (and never log a clean
    success for a rejected response): validation lives inside llm_call_fn."""
    client, stubber = make_client_and_stubber()
    bad = {"selected_candidate_number": 2, "selection_confidence": 950}
    for _ in range(2):
        stubber.add_response("converse", converse_response(bad), expected_converse("p"))
    seen = {}

    class FakeBT:
        def traced_call(self, name, input_data, llm_call_fn, prompt=None, metadata=None):
            try:
                return llm_call_fn()
            except Exception as e:
                seen["raised_inside"] = type(e).__name__
                raise

    with (
        stubber,
        patch("bedrock_clients.structured.braintrust_enabled", return_value=True),
        patch("bedrock_clients.structured.get_braintrust_client", return_value=FakeBT()),
    ):
        llm = make_llm(client)
        with pytest.raises(StructuredOutputError):
            llm.generate_structured_content(prompt="p", response_schema=SCHEMA, trace_name="t")
    assert seen["raised_inside"] == "StructuredOutputError"


def test_throttling_retried_then_succeeds():
    client, stubber = make_client_and_stubber()
    payload = {"selected_candidate_number": 1, "selection_confidence": 80}
    stubber.add_client_error("converse", service_error_code="ThrottlingException")
    stubber.add_response("converse", converse_response(payload), expected_converse("p"))
    with stubber, patch("bedrock_clients._retry.random.uniform", return_value=0.0):
        llm = make_llm(client)
        out = llm.generate_structured_content(prompt="p", response_schema=SCHEMA)
    assert out == payload


def test_access_denied_not_retried():
    client, stubber = make_client_and_stubber()
    stubber.add_client_error("converse", service_error_code="AccessDeniedException")
    with stubber:
        llm = make_llm(client)
        with pytest.raises(Exception) as excinfo:
            llm.generate_structured_content(prompt="p", response_schema=SCHEMA)
    assert "AccessDeniedException" in str(excinfo.value)
    stubber.assert_no_pending_responses()


def test_usage_math():
    client, stubber = make_client_and_stubber()
    payload = {"selected_candidate_number": 0, "selection_confidence": 50}
    stubber.add_response("converse", converse_response(payload), expected_converse("p"))
    with stubber:
        llm = make_llm(client)
        llm.generate_structured_content(prompt="p", response_schema=SCHEMA)
    stats = llm.get_usage_stats()
    assert stats["total_cost"] == pytest.approx(800 * 1.00 / 1_000_000 + 300 * 5.00 / 1_000_000)
    assert stats["completion_tokens"] == 300


def test_fingerprint_normalizes_only_the_candidate_bound():
    wider_candidates = {
        "type": "object",
        "properties": {
            "selected_candidate_number": {"type": "number", "minimum": 0, "maximum": 13},
            "selection_confidence": {"type": "number", "minimum": 0, "maximum": 100},
        },
        "required": ["selected_candidate_number", "selection_confidence"],
    }
    drifted_confidence = {
        "type": "object",
        "properties": {
            "selected_candidate_number": {"type": "number", "minimum": 0, "maximum": 3},
            "selection_confidence": {"type": "number", "minimum": 0, "maximum": 1},
        },
        "required": ["selected_candidate_number", "selection_confidence"],
    }
    assert _schema_fingerprint(SCHEMA) == _schema_fingerprint(wider_candidates)
    assert _schema_fingerprint(SCHEMA) != _schema_fingerprint(drifted_confidence)


def test_resolved_config_shape():
    client, stubber = make_client_and_stubber()
    payload = {"selected_candidate_number": 1, "selection_confidence": 70}
    stubber.add_response("converse", converse_response(payload), expected_converse("a"))
    with stubber:
        llm = make_llm(client)
        llm.generate_structured_content(prompt="a", response_schema=SCHEMA)
    cfg = llm.resolved_config()
    assert cfg["model_id"] == MODEL_ID
    assert cfg["operation"] == "Converse"
    assert cfg["thinking"] == "off"
    assert cfg["temperature"] == 0.0
    assert cfg["max_tokens"] == 2048
    assert cfg["tool_name"] == "emit_match_selection"
    assert cfg["output_shape_retries"] == 1
    assert cfg["schema_fingerprint"] == _schema_fingerprint(SCHEMA)
