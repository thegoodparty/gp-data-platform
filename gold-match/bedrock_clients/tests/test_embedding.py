import io
import json
from unittest.mock import patch

import boto3
import pytest
from botocore.config import Config
from botocore.response import StreamingBody
from botocore.stub import Stubber

from bedrock_clients.embedding import BedrockEmbeddingClient, EmbeddingValidationError

TITAN_ID = "amazon.titan-embed-text-v2:0"
NOVA_ID = "amazon.nova-2-multimodal-embeddings-v1:0"


def make_client_and_stubber():
    client = boto3.client(
        "bedrock-runtime",
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(retries={"max_attempts": 1, "mode": "standard"}),
    )
    return client, Stubber(client)


def response_body(payload: dict) -> dict:
    raw = json.dumps(payload).encode()
    return {"body": StreamingBody(io.BytesIO(raw), len(raw)), "contentType": "application/json"}


def titan_expected(text: str, dims: int = 8) -> dict:
    return {
        "modelId": TITAN_ID,
        "contentType": "application/json",
        "accept": "application/json",
        "body": json.dumps({"inputText": text, "dimensions": dims, "normalize": True}),
    }


def nova_expected(text: str, purpose: str, dims: int = 8) -> dict:
    return {
        "modelId": NOVA_ID,
        "contentType": "application/json",
        "accept": "application/json",
        "body": json.dumps(
            {
                "taskType": "SINGLE_EMBEDDING",
                "singleEmbeddingParams": {
                    "embeddingPurpose": purpose,
                    "embeddingDimension": dims,
                    "text": {"truncationMode": "NONE", "value": text},
                },
            }
        ),
    }


def make_titan(stub_client, **kwargs) -> BedrockEmbeddingClient:
    return BedrockEmbeddingClient(model="titan", dimensions=8, bedrock_runtime=stub_client, **kwargs)


def make_nova(stub_client, **kwargs) -> BedrockEmbeddingClient:
    return BedrockEmbeddingClient(model="nova", dimensions=8, bedrock_runtime=stub_client, **kwargs)


def test_titan_single_text_shape_params_and_cost():
    client, stubber = make_client_and_stubber()
    vec = [0.1] * 8
    stubber.add_response(
        "invoke_model", response_body({"embedding": vec, "inputTextTokenCount": 7}), titan_expected("hello")
    )
    with stubber:
        emb = make_titan(client)
        out = emb.create_embeddings(["hello"])
    assert out.shape == (1, 8)
    stats = emb.get_cost_stats()
    assert stats["total_embeddings_created"] == 1
    assert stats["total_input_tokens"] == 7
    assert stats["total_cost"] == pytest.approx(7 * 0.02 / 1_000_000)
    assert stats["estimated"] is False


def test_titan_batch_order_preserved():
    client, stubber = make_client_and_stubber()
    texts = ["a", "b", "c"]
    for i, t in enumerate(texts):
        vec = [0.0] * 8
        vec[i] = 1.0
        stubber.add_response(
            "invoke_model", response_body({"embedding": vec, "inputTextTokenCount": 1}), titan_expected(t)
        )
    with stubber:
        # max_concurrency=1 so the stubbed responses are consumed in order
        emb = make_titan(client, max_concurrency=1)
        out = emb.create_embeddings(texts)
    assert out.shape == (3, 8)
    assert out[0][0] == 1.0 and out[1][1] == 1.0 and out[2][2] == 1.0


def test_nova_purpose_dispatch_single_vs_batch():
    client, stubber = make_client_and_stubber()
    vec = [0.5] * 8
    stubber.add_response(
        "invoke_model",
        response_body({"embeddings": [{"embeddingType": "TEXT", "embedding": vec}]}),
        nova_expected("query text", "TEXT_RETRIEVAL"),
    )
    for t in ["doc one", "doc two"]:
        stubber.add_response(
            "invoke_model",
            response_body({"embeddings": [{"embeddingType": "TEXT", "embedding": vec}]}),
            nova_expected(t, "GENERIC_INDEX"),
        )
    with stubber:
        emb = make_nova(client, max_concurrency=1)
        single = emb.create_embeddings(["query text"])
        batch = emb.create_embeddings(["doc one", "doc two"])
    assert single.shape == (1, 8)
    assert batch.shape == (2, 8)
    assert emb.get_cost_stats()["estimated"] is True


def test_wrong_dimension_raises():
    client, stubber = make_client_and_stubber()
    stubber.add_response(
        "invoke_model", response_body({"embedding": [0.1] * 5, "inputTextTokenCount": 2}), titan_expected("x")
    )
    with stubber:
        emb = make_titan(client)
        with pytest.raises(EmbeddingValidationError, match="dimension"):
            emb.create_embeddings(["x"])


def test_zero_vector_raises():
    client, stubber = make_client_and_stubber()
    stubber.add_response(
        "invoke_model", response_body({"embedding": [0.0] * 8, "inputTextTokenCount": 2}), titan_expected("x")
    )
    with stubber:
        emb = make_titan(client)
        with pytest.raises(EmbeddingValidationError, match="norm"):
            emb.create_embeddings(["x"])


def test_billable_counted_even_when_validation_rejects():
    client, stubber = make_client_and_stubber()
    stubber.add_response(
        "invoke_model", response_body({"embedding": [0.0] * 8, "inputTextTokenCount": 9}), titan_expected("x")
    )
    with stubber:
        emb = make_titan(client)
        with pytest.raises(EmbeddingValidationError):
            emb.create_embeddings(["x"])
    assert emb.get_cost_stats()["total_input_tokens"] == 9


def test_throttling_retried_then_succeeds():
    client, stubber = make_client_and_stubber()
    stubber.add_client_error("invoke_model", service_error_code="ThrottlingException")
    stubber.add_response(
        "invoke_model", response_body({"embedding": [0.2] * 8, "inputTextTokenCount": 3}), titan_expected("y")
    )
    with stubber, patch("bedrock_clients._retry.random.uniform", return_value=0.0):
        emb = make_titan(client)
        out = emb.create_embeddings(["y"])
    assert out.shape == (1, 8)


def test_limiter_permit_per_physical_attempt():
    """Each physical attempt hits the quota, so each must take its own
    permit -- a retried call under one permit would amplify a throttling
    wave instead of pacing through it."""
    client, stubber = make_client_and_stubber()
    stubber.add_client_error("invoke_model", service_error_code="ThrottlingException")
    stubber.add_response(
        "invoke_model", response_body({"embedding": [0.2] * 8, "inputTextTokenCount": 3}), titan_expected("y")
    )
    with stubber, patch("bedrock_clients._retry.random.uniform", return_value=0.0):
        emb = make_titan(client)
        acquires = []
        real_acquire = emb._limiter.acquire
        emb._limiter.acquire = lambda **kw: (acquires.append(1), real_acquire(**kw))[1]
        emb.create_embeddings(["y"])
    assert len(acquires) == 2


def test_validation_exception_not_retried():
    client, stubber = make_client_and_stubber()
    stubber.add_client_error("invoke_model", service_error_code="ValidationException")
    with stubber:
        emb = make_titan(client)
        with pytest.raises(Exception) as excinfo:
            emb.create_embeddings(["y"])
    assert "ValidationException" in str(excinfo.value)
    stubber.assert_no_pending_responses()


def test_resolved_config_shape():
    client, _ = make_client_and_stubber()
    emb = make_nova(client, requests_per_minute=2000, tokens_per_minute=None)
    cfg = emb.resolved_config()
    assert cfg["provider"] == "bedrock"
    assert cfg["region"] == "us-east-1"
    assert cfg["model_id"] == NOVA_ID
    assert cfg["operation"] == "InvokeModel"
    assert cfg["dimensions"] == 8
    assert cfg["embedding_purpose"] == {"batch": "GENERIC_INDEX", "single": "TEXT_RETRIEVAL"}
    assert cfg["requests_per_minute"] == 2000
    assert cfg["usage_is_estimated"] is True
    titan = make_titan(client)
    tcfg = titan.resolved_config()
    assert tcfg["normalize"] is True
    assert tcfg["embedding_purpose"] is None
    assert tcfg["usage_is_estimated"] is False
