"""Retry policy shared by both Bedrock clients: throttling and transient AWS
errors only, full jitter, and a hard elapsed-time ceiling. Deliberately NOT
the incumbent Gemini clients' retry-everything pattern -- a validation or
access error is a fact, not weather.
"""

import random
import time

from botocore.exceptions import ClientError, ConnectionError, HTTPClientError

# Derived from the bedrock-runtime service model's error shapes for
# InvokeModel/Converse (ModelTimeoutException is its 408; retry per AWS
# guidance). TooManyRequestsException is not a bedrock-runtime shape.
RETRYABLE_ERROR_CODES = frozenset(
    {
        "ThrottlingException",
        "ServiceUnavailableException",
        "ModelNotReadyException",
        "ModelTimeoutException",
        "InternalServerException",
    }
)


def _is_retryable(exc: Exception) -> bool:
    # HTTPClientError is botocore's transport-failure family (read timeouts,
    # connection-closed, response-streaming errors) and is NOT a
    # ConnectionError subclass -- verified against botocore's MRO.
    if isinstance(exc, ConnectionError | HTTPClientError):
        return True
    if isinstance(exc, ClientError):
        return exc.response.get("Error", {}).get("Code") in RETRYABLE_ERROR_CODES
    return False


def call_with_retries(fn, *, max_retries: int, max_elapsed_seconds: float):
    """Call `fn()` retrying only retryable failures, with full jitter
    (uniform over an exponentially growing cap) and a wall-clock ceiling the
    sleep itself may not cross -- a delay that would land past the deadline
    raises instead of sleeping and attempting once more."""
    start = time.monotonic()
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            attempt += 1
            if attempt > max_retries:
                raise
            delay = random.uniform(0, min(30.0, 0.5 * 2**attempt))
            if time.monotonic() - start + delay > max_elapsed_seconds:
                raise
            time.sleep(delay)
