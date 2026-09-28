"""Concurrency and rate limiting for the Bedrock clients.

One semaphore bounds in-flight calls (sized with the botocore connection
pool), a strict request bucket paces calls to the model's requests-per-minute
quota, and an optional token bucket paces to its tokens-per-minute quota --
Titan's 300k tokens/min is the binding constraint on a full re-match, and AWS
throttles embeddings by RPM per its docs, so both are enforced. Token debits
happen before the call from an estimate; `reconcile` settles the difference
once the response reports the real count.
"""

import threading
import time
from contextlib import contextmanager


class RateLimiter:
    def __init__(self, max_concurrency: int, requests_per_minute: int, tokens_per_minute: int | None):
        if tokens_per_minute is not None and tokens_per_minute <= 0:
            # A falsy 0 would silently mean "unlimited"; reject it instead.
            raise ValueError(f"tokens_per_minute must be positive or None, got {tokens_per_minute}")
        self.seconds_per_request = 60.0 / requests_per_minute
        self._semaphore = threading.Semaphore(max_concurrency)
        self._lock = threading.Lock()
        # Strict pacing for requests: capacity 1, so calls smooth out to the
        # per-minute rate instead of bursting the whole minute up front.
        self._request_level = 1.0
        self._request_refill_per_sec = requests_per_minute / 60.0
        self._tpm_capacity = float(tokens_per_minute) if tokens_per_minute else None
        self._token_level = self._tpm_capacity
        self._last_refill = time.monotonic()

    def _refill_locked(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._request_level = min(1.0, self._request_level + elapsed * self._request_refill_per_sec)
        if self._tpm_capacity is not None:
            self._token_level = min(self._tpm_capacity, self._token_level + elapsed * self._tpm_capacity / 60.0)

    def _wait_for_capacity(self, estimated_tokens: int) -> None:
        if self._tpm_capacity is not None and estimated_tokens > self._tpm_capacity:
            # The bucket can never hold this much; waiting would hang forever.
            raise ValueError(
                f"estimated_tokens {estimated_tokens} exceeds the tokens-per-minute "
                f"capacity {self._tpm_capacity:.0f}; this call can never acquire"
            )
        while True:
            with self._lock:
                self._refill_locked()
                tokens_ok = self._tpm_capacity is None or self._token_level >= estimated_tokens
                if self._request_level >= 1.0 and tokens_ok:
                    self._request_level -= 1.0
                    if self._tpm_capacity is not None:
                        self._token_level -= estimated_tokens
                    return
            time.sleep(min(self.seconds_per_request, 0.05))

    @contextmanager
    def acquire(self, estimated_tokens: int = 0):
        with self._semaphore:
            self._wait_for_capacity(estimated_tokens)
            yield

    def reconcile(self, estimated_tokens: int, actual_tokens: int) -> None:
        """Settle the token bucket once the response reports real usage."""
        if self._tpm_capacity is None:
            return
        with self._lock:
            self._refill_locked()
            self._token_level = min(self._tpm_capacity, self._token_level - (actual_tokens - estimated_tokens))

    def tokens_available(self) -> float | None:
        if self._tpm_capacity is None:
            return None
        with self._lock:
            self._refill_locked()
            return self._token_level
