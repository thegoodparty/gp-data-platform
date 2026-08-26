"""Extraction helpers for the extract_ballotready DAG.

Pulls BallotReady (CivicEngine) GraphQL objects by id and lands the raw node
payloads in Databricks. Every entity is addressed the same way, through
`nodes(ids:)` over base64 global ids, so one client and one registry cover all
of them.
"""

import logging
import random
import threading
import time
from base64 import b64encode
from collections.abc import Callable, Iterator, Mapping
from typing import Any

logger = logging.getLogger("airflow.task")

CIVIC_ENGINE_GRAPHQL_URL = "https://bpi.civicengine.com/graphql"
_NODE_ID_PREFIX = "gid://ballot-factory"


def encode_node_id(node_type: str, node_id: int) -> str:
    """Encode an integer BallotReady id as its base64 GraphQL global id."""
    return b64encode(f"{_NODE_ID_PREFIX}/{node_type}/{node_id}".encode()).decode("utf-8")


def chunked(seq: list[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive `size`-length chunks of `seq`."""
    if size < 1:
        raise ValueError(f"chunk size must be >= 1, got {size}")
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


class RateLimiter:
    """Caps request rate across threads, and lets any thread pause all of them.

    Worker count bounds how much is in flight; this bounds the load we actually
    put on CivicEngine. The pause exists because a 429 handled by one worker
    alone just means the other workers keep earning more of them.
    """

    def __init__(
        self,
        requests_per_second: float,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        if requests_per_second <= 0:
            raise ValueError(f"requests_per_second must be > 0, got {requests_per_second}")
        self._interval = 1.0 / requests_per_second
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        """Block until this thread may issue a request."""
        with self._lock:
            wait = self._next_allowed - self._clock()
            if wait > 0:
                self._sleep(wait)
            self._next_allowed = self._clock() + self._interval

    def pause_for(self, seconds: float) -> None:
        """Hold every worker for `seconds`, after a rate-limit response."""
        with self._lock:
            self._next_allowed = max(self._next_allowed, self._clock() + seconds)


def is_retryable_status(status_code: int) -> bool:
    """429 (rate limited) and 5xx (server) responses are worth retrying."""
    return status_code == 429 or status_code >= 500


def retry_wait_seconds(
    headers: Mapping[str, str],
    attempt: int,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
    rng: Callable[[float, float], float] = random.uniform,
) -> float:
    """Seconds to wait before the next retry.

    Honors a numeric `Retry-After` when present; otherwise exponential backoff
    with full jitter so concurrent workers do not resynchronize on the retry.
    """
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if retry_after:
        try:
            return max(0.0, min(float(retry_after), max_backoff))
        except ValueError:
            pass
    return rng(0, min(base_backoff * (2**attempt), max_backoff))
