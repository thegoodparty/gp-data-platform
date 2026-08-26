"""Bedrock structured-content client: Claude Haiku 4.5 via the Converse API
on the account's GLOBAL cross-region inference profile, with ONE forced named
tool carrying the caller's JSON schema.

The forced tool is the transport that guarantees schema-shaped output on
every profile; a native JSON-schema response mode, if the profile turns out
to support one, is an internal swap inside this class and changes nothing
for callers. The complete tool input is validated with jsonschema and ANY
miss raises StructuredOutputError -- the matcher's technical-failure
contract: a run fails rather than recording a made-up answer. Thinking
stays off: forced tool choice is incompatible with extended thinking, and
the incumbent ran at minimal thinking with temperature 0.

Usage accounting reads the response's usage block (thread-safe) and counts
billable calls even when their output is later rejected.
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

_TOOL_NAME = "emit_match_selection"
# Haiku 4.5 on Bedrock, us-east-1 on-demand, per 1M tokens.
_PRICE_PER_MTOK = {"input": 1.00, "output": 5.00}


class StructuredOutputError(Exception):
    pass


def _schema_fingerprint(schema: dict) -> str:
    """sha256 over the schema with numeric minimum/maximum bounds normalized
    out: the matcher's schema varies only in its candidate-count bound per
    call, and the fingerprint pins the structure, not the bound."""

    def normalize(node):
        if isinstance(node, dict):
            return {
                k: ("<bound>" if k in ("maximum", "minimum") and isinstance(v, int | float) else normalize(v))
                for k, v in node.items()
            }
        if isinstance(node, list):
            return [normalize(v) for v in node]
        return node

    return hashlib.sha256(json.dumps(normalize(schema), sort_keys=True).encode()).hexdigest()


class BedrockStructuredContentClient:
    def __init__(
        self,
        model_id: str = "global.anthropic.claude-haiku-4-5-20251001-v1:0",
        region: str = "us-east-1",
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_concurrency: int = 100,
        requests_per_minute: int = 10_000,
        tokens_per_minute: int | None = 5_000_000,
        max_retries: int = 6,
        max_elapsed_seconds: float = 120.0,
        bedrock_runtime=None,
    ):
        self.model_id = model_id
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

    def _converse(self, prompt: str, response_schema: dict) -> dict:
        estimated_tokens = max(1, len(prompt) // 4) + self.max_tokens
        with self._limiter.acquire(estimated_tokens=estimated_tokens):
            response = call_with_retries(
                lambda: self._client.converse(
                    modelId=self.model_id,
                    messages=[{"role": "user", "content": [{"text": prompt}]}],
                    inferenceConfig={"temperature": self.temperature, "maxTokens": self.max_tokens},
                    toolConfig={
                        "tools": [
                            {
                                "toolSpec": {
                                    "name": _TOOL_NAME,
                                    "description": "Return the selection as arguments to this tool.",
                                    "inputSchema": {"json": response_schema},
                                }
                            }
                        ],
                        "toolChoice": {"tool": {"name": _TOOL_NAME}},
                    },
                ),
                max_retries=self.max_retries,
                max_elapsed_seconds=self.max_elapsed_seconds,
            )
        usage = response.get("usage", {})
        actual = usage.get("totalTokens", estimated_tokens)
        self._limiter.reconcile(estimated_tokens=estimated_tokens, actual_tokens=actual)
        return response

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
        self._last_schema_fingerprint = _schema_fingerprint(response_schema)

        def llm_fn() -> dict:
            return self._converse(prompt, response_schema)

        if braintrust_enabled():
            response = get_braintrust_client().traced_call(
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
        else:
            response = llm_fn()

        # Billable before validated: record usage even for rejected output.
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

        stop_reason = response.get("stopReason")
        if stop_reason != "tool_use":
            raise StructuredOutputError(
                f"expected the forced tool call, got stopReason={stop_reason!r} -- a technical failure, not an abstention"
            )
        content = response.get("output", {}).get("message", {}).get("content", [])
        tool_use = next((block["toolUse"] for block in content if "toolUse" in block), None)
        if tool_use is None:
            raise StructuredOutputError("stopReason was tool_use but no toolUse block was returned")
        result = tool_use.get("input")
        try:
            jsonschema.validate(instance=result, schema=response_schema)
        except jsonschema.ValidationError as e:
            raise StructuredOutputError(f"tool input failed schema validation: {e.message}") from e
        return result

    def get_usage_stats(self) -> dict:
        with self._usage_lock:
            return {
                "api_calls": self.api_call_count,
                "prompt_tokens": self.total_prompt_tokens,
                "completion_tokens": self.total_completion_tokens,
                "total_cost": self.total_cost,
            }

    def resolved_config(self) -> dict:
        return {
            "provider": "bedrock",
            "region": self.region,
            "model_id": self.model_id,
            "operation": "Converse",
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "thinking": "off",
            "tool_name": _TOOL_NAME,
            "schema_fingerprint": self._last_schema_fingerprint,
            "max_concurrency": self.max_concurrency,
            "requests_per_minute": self.requests_per_minute,
            "tokens_per_minute": self.tokens_per_minute,
            "max_retries": self.max_retries,
        }
