"""Bedrock structured-content client: Claude Haiku 4.5 via the Converse API
on the account's GLOBAL cross-region inference profile, in NATIVE JSON-schema
output mode (additionalModelRequestFields.output_config).

Native mode replaced the forced named tool after the 2026-08-28 dry runs: the
forced tool deterministically omitted a required property on a small class of
offices (identical re-sends missed identically at temperature 0), while native
constrained decoding fixed every probed failure on the exact failing prompts.
Constrained decoding cannot carry numeric minimum/maximum bounds (the service
rejects them), so the WIRE schema is the caller's schema with numeric bounds
stripped and additionalProperties pinned false, and the FULL schema is still
enforced post-hoc with jsonschema -- an out-of-bounds value raises exactly as
before. An output-shape miss (truncation, unparseable text, schema violation)
gets exactly ONE re-ask, and a second miss raises StructuredOutputError: the
matcher's technical-failure contract, never an abstention. Thinking stays off
(the incumbent ran minimal-thinking at temperature 0).

Usage accounting reads each response's usage block (thread-safe) and counts
every billable response, including ones whose output is then rejected. The
model id is deliberately NOT configurable: the pricing constants below are
this model's, and a different model silently mispricing itself is worse
than adding a priced, tested id when one is actually needed.
"""

import hashlib
import json
import os
import threading

import jsonschema

from bedrock_clients._retry import call_with_retries
from bedrock_clients.throttle import RateLimiter
from shared.braintrust import get_client as get_braintrust_client
from shared.braintrust import is_enabled as braintrust_enabled

_MODEL_ID = "global.anthropic.claude-haiku-4-5-20251001-v1:0"
# Haiku 4.5 on Bedrock, us-east-1 on-demand, per 1M tokens.
_PRICE_PER_MTOK = {"input": 1.00, "output": 5.00}
# One re-ask for output-shape misses; the second miss raises.
_OUTPUT_SHAPE_RETRIES = 1


def _strip_numeric_bounds(node):
    """The wire copy of the schema for native mode: constrained decoding
    rejects numeric minimum/maximum (live ValidationException), so bounds are
    removed here and enforced post-hoc against the caller's FULL schema."""
    if isinstance(node, dict):
        return {k: _strip_numeric_bounds(v) for k, v in node.items() if k not in ("minimum", "maximum")}
    if isinstance(node, list):
        return [_strip_numeric_bounds(v) for v in node]
    return node


class StructuredOutputError(Exception):
    pass


def _schema_fingerprint(schema: dict) -> str:
    """sha256 over the schema with ONLY the candidate-count bound normalized
    out (the `maximum` values inside the selected_candidate_number property,
    which vary per call with menu size). Any other bound change -- e.g. a
    confidence maximum drifting -- is meaningful and stays in the hash."""

    def normalize(node, in_candidate_property=False):
        if isinstance(node, dict):
            return {
                k: (
                    "<bound>"
                    if in_candidate_property and k == "maximum" and isinstance(v, int | float)
                    else normalize(v, in_candidate_property or k == "selected_candidate_number")
                )
                for k, v in node.items()
            }
        if isinstance(node, list):
            return [normalize(v, in_candidate_property) for v in node]
        return node

    return hashlib.sha256(json.dumps(normalize(schema), sort_keys=True).encode()).hexdigest()


class BedrockStructuredContentClient:
    def __init__(
        self,
        region: str = "us-east-1",
        temperature: float = 0.0,
        max_tokens: int = 2048,
        max_concurrency: int = 100,
        requests_per_minute: int = 10_000,
        tokens_per_minute: int | None = 5_000_000,
        max_retries: int = 6,
        max_elapsed_seconds: float = 120.0,
        bedrock_runtime=None,
    ):
        self.model_id = _MODEL_ID
        self.region = region
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_concurrency = max_concurrency
        self.requests_per_minute = requests_per_minute
        self.tokens_per_minute = tokens_per_minute
        self.max_retries = max_retries
        self.max_elapsed_seconds = max_elapsed_seconds
        self._limiter = RateLimiter(max_concurrency, requests_per_minute, tokens_per_minute)
        self._usage_lock = threading.Lock()
        self.api_call_count = 0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0
        self.total_cost = 0.0
        self._last_schema_fingerprint: str | None = None
        if bedrock_runtime is not None:
            self._client = bedrock_runtime
        else:
            import boto3
            from botocore.config import Config

            # Our retry policy is the only one; pool sized to the cap.
            self._client = boto3.client(
                "bedrock-runtime",
                region_name=region,
                config=Config(
                    max_pool_connections=max_concurrency,
                    retries={"max_attempts": 1, "mode": "standard"},
                ),
            )

    def _record_usage(self, response: dict) -> None:
        # Billable before validated: every returned response was paid for.
        usage = response.get("usage", {})
        prompt_tokens = usage.get("inputTokens", 0)
        completion_tokens = usage.get("outputTokens", 0)
        with self._usage_lock:
            self.api_call_count += 1
            self.total_prompt_tokens += prompt_tokens
            self.total_completion_tokens += completion_tokens
            self.total_cost += (
                prompt_tokens * _PRICE_PER_MTOK["input"] + completion_tokens * _PRICE_PER_MTOK["output"]
            ) / 1_000_000

    def _extract_and_validate(self, response: dict, response_schema: dict) -> dict:
        stop_reason = response.get("stopReason")
        if stop_reason != "end_turn":
            raise StructuredOutputError(
                f"expected a completed native-schema response, got stopReason={stop_reason!r} "
                "-- a technical failure, not an abstention"
            )
        content = response.get("output", {}).get("message", {}).get("content", [])
        text = next((block["text"] for block in content if "text" in block), None)
        if text is None:
            raise StructuredOutputError("stopReason was end_turn but no text block was returned")
        try:
            result = json.loads(text)
        except json.JSONDecodeError as e:
            raise StructuredOutputError(f"native-schema response was not valid JSON: {e}") from e
        try:
            # The FULL schema, bounds included -- the wire schema cannot carry
            # numeric bounds, so this is where an out-of-range value raises.
            jsonschema.validate(instance=result, schema=response_schema)
        except jsonschema.ValidationError as e:
            raise StructuredOutputError(f"response failed schema validation: {e.message}") from e
        return result

    def generate_structured_content(
        self,
        prompt: str,
        response_schema: dict,
        trace_name: str | None = None,
        **kwargs,
    ) -> dict:
        """Extra kwargs from the incumbent client's signature (model,
        temperature overrides, thinking levels) are accepted and ignored --
        this client is single-model, single-config by design; the resolved
        config records what ran."""
        fingerprint = _schema_fingerprint(response_schema)
        with self._usage_lock:
            self._last_schema_fingerprint = fingerprint
        estimated_tokens = max(1, len(prompt) // 4) + self.max_tokens

        wire_schema = {**_strip_numeric_bounds(response_schema), "additionalProperties": False}

        def attempt() -> dict:
            # One permit per PHYSICAL attempt; each hits the quota.
            with self._limiter.acquire(estimated_tokens=estimated_tokens):
                return self._client.converse(
                    modelId=self.model_id,
                    messages=[{"role": "user", "content": [{"text": prompt}]}],
                    inferenceConfig={"temperature": self.temperature, "maxTokens": self.max_tokens},
                    additionalModelRequestFields={
                        "output_config": {"format": {"type": "json_schema", "schema": wire_schema}}
                    },
                )

        def llm_fn() -> dict:
            # Usage recording, extraction, and validation all live INSIDE the
            # traced callable: a truncated or schema-invalid response must
            # trace as the failure it is, and the traced output is the parsed
            # selection (matching the incumbent), not the raw AWS envelope.
            last_error: StructuredOutputError | None = None
            for _ in range(1 + _OUTPUT_SHAPE_RETRIES):
                response = call_with_retries(
                    attempt, max_retries=self.max_retries, max_elapsed_seconds=self.max_elapsed_seconds
                )
                usage = response.get("usage", {})
                self._limiter.reconcile(
                    estimated_tokens=estimated_tokens,
                    actual_tokens=usage.get("totalTokens", estimated_tokens),
                )
                self._record_usage(response)
                try:
                    return self._extract_and_validate(response, response_schema)
                except StructuredOutputError as e:
                    last_error = e
            raise last_error

        if braintrust_enabled():
            return get_braintrust_client().traced_call(
                name=trace_name or "generate_structured_content",
                input_data={"prompt": prompt},
                llm_call_fn=llm_fn,
                prompt=prompt,
                metadata={
                    "model": self.model_id,
                    "temperature": self.temperature,
                    "environment": os.getenv("ENVIRONMENT", "local"),
                },
            )
        return llm_fn()

    def get_usage_stats(self) -> dict:
        with self._usage_lock:
            return {
                "api_calls": self.api_call_count,
                "prompt_tokens": self.total_prompt_tokens,
                "completion_tokens": self.total_completion_tokens,
                "total_cost": self.total_cost,
            }

    def resolved_config(self) -> dict:
        with self._usage_lock:
            fingerprint = self._last_schema_fingerprint
        return {
            "provider": "bedrock",
            "region": self.region,
            "model_id": self.model_id,
            "operation": "Converse",
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "thinking": "off",
            "output_mode": "native_json_schema",
            "output_shape_retries": _OUTPUT_SHAPE_RETRIES,
            "schema_fingerprint": fingerprint,
            "max_concurrency": self.max_concurrency,
            "requests_per_minute": self.requests_per_minute,
            "tokens_per_minute": self.tokens_per_minute,
            "max_retries": self.max_retries,
        }
