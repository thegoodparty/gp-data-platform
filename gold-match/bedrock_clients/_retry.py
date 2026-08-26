"""Retry policy shared by both Bedrock clients: throttling and transient AWS
errors only, full jitter, and a hard elapsed-time ceiling. Deliberately NOT
the incumbent Gemini clients' retry-everything pattern -- a validation or
access error is a fact, not weather.
"""

import random
import time

from botocore.exceptions import ClientError, ConnectionError, ReadTimeoutError

RETRYABLE_ERROR_CODES = frozenset(
    {
        "ThrottlingException",
        "TooManyRequestsException",
        "ServiceUnavailableException",
        "ModelNotReadyException",
        "InternalServerException",
    }
)


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, ConnectionError | ReadTimeoutError):
        return True
    if isinstance(exc, ClientError):
        return exc.response.get("Error", {}).get("Code") in RETRYABLE_ERROR_CODES
    return False


def call_with_retries(fn, *, max_retries: int, max_elapsed_seconds: float):
    """Call `fn()` retrying only retryable failures, with full jitter
    (uniform over an exponentially growing cap) and a wall-clock ceiling."""
    start = time.monotonic()
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:
            if not _is_retryable(exc):
                raise
            attempt += 1
            if attempt > max_retries or time.monotonic() - start > max_elapsed_seconds:
                raise
            time.sleep(random.uniform(0, min(30.0, 0.5 * 2**attempt)))
