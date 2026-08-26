"""Bedrock embedding client: Titan Text Embeddings V2 primary, Nova 2
Multimodal Embeddings selectable, one text per InvokeModel call.

Calling convention (mirrors the matcher's, documented at its
_embed_query_texts): a MULTI-text call embeds documents being indexed, a
SINGLE-text call embeds a query. Titan has no purpose axis (symmetric);
Nova does, so the dispatch maps batch -> GENERIC_INDEX and single ->
TEXT_RETRIEVAL. Nova's response carries no token count, so its usage
accounting is a character-based ESTIMATE and is flagged as such in both
get_cost_stats() and resolved_config(); Titan's is response-based
(inputTextTokenCount).
"""

import json
import threading
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from bedrock_clients._retry import call_with_retries
from bedrock_clients.throttle import RateLimiter

_MODEL_IDS = {
    "titan": "amazon.titan-embed-text-v2:0",
    "nova": "amazon.nova-2-multimodal-embeddings-v1:0",
}
_PRICE_PER_MTOK_INPUT = {"titan": 0.02, "nova": 0.135}
_NOVA_PURPOSE = {"batch": "GENERIC_INDEX", "single": "TEXT_RETRIEVAL"}


class EmbeddingValidationError(Exception):
    pass


def _estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)


class BedrockEmbeddingClient:
    def __init__(
        self,
        model: str = "titan",
        region: str = "us-east-1",
        dimensions: int = 1024,
        max_concurrency: int = 20,
        requests_per_minute: int = 6000,
        tokens_per_minute: int | None = 300_000,
        max_retries: int = 6,
        max_elapsed_seconds: float = 120.0,
        bedrock_runtime=None,
    ):
        if model not in _MODEL_IDS:
            raise ValueError(f"model must be one of {sorted(_MODEL_IDS)}, got {model!r}")
        self.model = model
        self.model_id = _MODEL_IDS[model]
        self.region = region
        self.dimensions = dimensions
        self.max_concurrency = max_concurrency
        self.requests_per_minute = requests_per_minute
        self.tokens_per_minute = tokens_per_minute
        self.max_retries = max_retries
        self.max_elapsed_seconds = max_elapsed_seconds
        self._limiter = RateLimiter(max_concurrency, requests_per_minute, tokens_per_minute)
        self._usage_lock = threading.Lock()
        self.total_embeddings_created = 0
        self.total_input_tokens = 0
        self.total_cost = 0.0
        if bedrock_runtime is not None:
            self._client = bedrock_runtime
        else:
            import boto3
            from botocore.config import Config

            # Our retry policy is the only one: botocore's own retries are off,
            # and the pool is sized to the concurrency cap so the semaphore is
            # what actually bounds in-flight connections.
            self._client = boto3.client(
                "bedrock-runtime",
                region_name=region,
                config=Config(
                    max_pool_connections=max_concurrency,
                    retries={"max_attempts": 1, "mode": "standard"},
                ),
            )

    # -- request/response shapes ---------------------------------------

    def _request_body(self, text: str, purpose_key: str) -> str:
        if self.model == "titan":
            return json.dumps({"inputText": text, "dimensions": self.dimensions, "normalize": True})
        return json.dumps(
            {
                "taskType": "SINGLE_EMBEDDING",
                "singleEmbeddingParams": {
                    "embeddingPurpose": _NOVA_PURPOSE[purpose_key],
                    "embeddingDimension": self.dimensions,
                    "text": {"truncationMode": "NONE", "value": text},
                },
            }
        )

    def _parse_response(self, payload: dict) -> tuple[list[float], int | None]:
        if self.model == "titan":
            return payload["embedding"], payload["inputTextTokenCount"]
        return payload["embeddings"][0]["embedding"], None

    def _validate_vector(self, vector: list[float], text: str) -> np.ndarray:
        arr = np.asarray(vector, dtype=np.float64)
        if arr.shape != (self.dimensions,):
            raise EmbeddingValidationError(
                f"embedding dimension {arr.shape} != ({self.dimensions},) for text {text[:80]!r}"
            )
        if not np.all(np.isfinite(arr)):
            raise EmbeddingValidationError(f"non-finite embedding values for text {text[:80]!r}")
        if float(np.linalg.norm(arr)) == 0.0:
            raise EmbeddingValidationError(f"zero-norm embedding for text {text[:80]!r}")
        return arr

    # -- the call path ---------------------------------------------------

    def _embed_one(self, text: str, purpose_key: str) -> np.ndarray:
        estimated = _estimate_tokens(text)
        with self._limiter.acquire(estimated_tokens=estimated):
            response = call_with_retries(
                lambda: self._client.invoke_model(
                    modelId=self.model_id,
                    contentType="application/json",
                    accept="application/json",
                    body=self._request_body(text, purpose_key),
                ),
                max_retries=self.max_retries,
                max_elapsed_seconds=self.max_elapsed_seconds,
            )
        payload = json.loads(response["body"].read())
        vector, reported_tokens = self._parse_response(payload)
        tokens = reported_tokens if reported_tokens is not None else estimated
        self._limiter.reconcile(estimated_tokens=estimated, actual_tokens=tokens)
        # Billable before validated: a rejected vector was still paid for.
        with self._usage_lock:
            self.total_embeddings_created += 1
            self.total_input_tokens += tokens
            self.total_cost += tokens * _PRICE_PER_MTOK_INPUT[self.model] / 1_000_000
        return self._validate_vector(vector, text)

    def create_embeddings(self, texts: list[str], **kwargs) -> np.ndarray:
        """Embed texts, one InvokeModel call each. `len(texts) == 1` is the
        query path, anything larger the document path (see module docstring).
        Extra kwargs from the incumbent client's signature are accepted and
        ignored -- concurrency here is governed by the limiter, not by
        per-call batch knobs."""
        if not texts:
            return np.empty((0, self.dimensions))
        if len(texts) == 1:
            return self._embed_one(texts[0], "single").reshape(1, -1)
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            rows = list(executor.map(lambda t: self._embed_one(t, "batch"), texts))
        if len(rows) != len(texts):
            raise EmbeddingValidationError(f"expected {len(texts)} embeddings, got {len(rows)}")
        return np.vstack(rows)

    # -- accounting and provenance ----------------------------------------

    def get_cost_stats(self) -> dict:
        with self._usage_lock:
            return {
                "total_embeddings_created": self.total_embeddings_created,
                "total_input_tokens": self.total_input_tokens,
                "total_cost": self.total_cost,
                "estimated": self.model == "nova",
            }

    def resolved_config(self) -> dict:
        return {
            "provider": "bedrock",
            "region": self.region,
            "model_id": self.model_id,
            "operation": "InvokeModel",
            "dimensions": self.dimensions,
            "normalize": True if self.model == "titan" else None,
            "truncation_mode": None if self.model == "titan" else "NONE",
            "embedding_purpose": _NOVA_PURPOSE.copy() if self.model == "nova" else None,
            "max_concurrency": self.max_concurrency,
            "requests_per_minute": self.requests_per_minute,
            "tokens_per_minute": self.tokens_per_minute,
            "max_retries": self.max_retries,
            "usage_is_estimated": self.model == "nova",
        }
