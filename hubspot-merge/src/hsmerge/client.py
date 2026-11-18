"""The three HubSpot calls the merge job makes, paced and retried, and nothing else.

Every shape here was recorded against the sandbox by `reverse-etl/probes/checks.py`
(check 13), and the notes below say which finding each one builds on, because the
classification of a pair depends on them:

- a merged-away id reads as its survivor, not as missing;
- a batch read answers one record per distinct resolved object, with no echo of the ids
  asked for and in no particular order, so a record is attributed to a request only
  through its own id and its `hs_merged_object_ids`;
- an id that is archived or never existed reads as `OBJECT_NOT_FOUND` in a 207 envelope;
- a merge that HubSpot refuses answers 400 or 404, which is a real answer about the pair
  and never worth a retry.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from tenacity import RetryCallState, Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from .config import Config

ACCOUNT_INFO_PATH = "/account-info/v3/details"
BATCH_READ_PATH = "/crm/v3/objects/contacts/batch/read"
MERGE_PATH = "/crm/v3/objects/contacts/merge"

# The read carries no echo of the ids requested, so the only way to learn that one
# resolved to something is this property on the record it resolved to. It is asked for on
# every read whatever property list the caller passes.
HS_MERGED_OBJECT_IDS = "hs_merged_object_ids"

REQUEST_TIMEOUT_SECONDS = 30.0
# Five attempts, 1 to 8 seconds apart, matches the reverse-etl destination.
RETRY_ATTEMPTS = 5
_EXPONENTIAL_BACKOFF = wait_exponential(multiplier=1, max=30)


class HubSpotError(RuntimeError):
    """A status the job cannot use, either final or after the retries ran out.

    The message names the status and HubSpot's own category so a `failed` log row says
    what happened. It never carries the request headers, so the token cannot reach a log.
    """

    def __init__(self, status_code: int, message: str, retry_after: float | None = None):
        self.status_code = status_code
        self.retry_after = retry_after
        super().__init__(f"HubSpot answered {status_code}: {message}")


class WrongPortalError(RuntimeError):
    """Stops the run rather than merge into a portal nobody sanctioned."""

    def __init__(self, actual: str, expected: str):
        super().__init__(f"credential opens portal {actual}, expected {expected}. Refusing to run.")


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    body: Mapping[str, Any]
    headers: Mapping[str, str] = field(default_factory=dict)


class HttpTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> HttpResponse: ...


class RequestsTransport:
    """The real transport. Not unit tested: it is the one call that must reach HubSpot."""

    def request(
        self,
        method: str,
        url: str,
        *,
        body: Mapping[str, Any] | None,
        headers: Mapping[str, str],
        timeout: float,
    ) -> HttpResponse:
        import requests

        response = requests.request(
            method, url, json=dict(body) if body is not None else None, headers=dict(headers), timeout=timeout
        )
        try:
            parsed = response.json()
        except ValueError:
            parsed = {}
        return HttpResponse(status_code=response.status_code, body=parsed, headers=dict(response.headers))


@dataclass(frozen=True)
class Record:
    """One contact as HubSpot returned it, with the ids that resolved to it.

    `merged_ids` is what HubSpot names in `hs_merged_object_ids`, and it is empty for a
    contact that was never merged, because HubSpot leaves that property null. A merged
    record may also name its own id there, and most do not; `ReadResult.resolved` adds the
    record's own id either way.
    """

    id: str
    merged_ids: tuple[str, ...]
    properties: Mapping[str, Any]


@dataclass(frozen=True)
class ReadResult:
    """A batch read, keyed by the record each id resolved to.

    Neither id of an already-merged pair is a key in `records`: the survivor's record is,
    and `resolved` names which requested ids landed on it. A requested id that resolved to
    nothing is in `missing_ids` and appears in no record.
    """

    records: Mapping[str, Record]
    missing_ids: tuple[str, ...]

    @property
    def resolved(self) -> Mapping[str, str]:
        """Every contact id the read can speak for, mapped to the record it came back as.

        The rule classification reads: a record speaks for its own id and for the ids it
        absorbed, and an id absent from this map did not resolve at all.
        """
        return {
            contact_id: record.id
            for record in self.records.values()
            for contact_id in (record.id, *record.merged_ids)
        }


def _retryable(error: BaseException) -> bool:
    """429 and 5xx only. Every other 4xx is HubSpot's answer about this pair."""
    return isinstance(error, HubSpotError) and (error.status_code == 429 or error.status_code >= 500)


def _wait(retry_state: RetryCallState) -> float:
    """`Retry-After` when HubSpot names it, exponential backoff otherwise."""
    outcome = retry_state.outcome
    error = outcome.exception() if outcome is not None else None
    if isinstance(error, HubSpotError) and error.retry_after is not None:
        return error.retry_after
    return _EXPONENTIAL_BACKOFF(retry_state)


def _retry_after_seconds(headers: Mapping[str, str]) -> float | None:
    # Header names are case-insensitive and requests keeps whatever casing the server sent.
    for name, value in headers.items():
        if name.lower() != "retry-after":
            continue
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _error_message(body: Mapping[str, Any]) -> str:
    category = body.get("category") or body.get("status") or "no category"
    return f"{category}: {body.get('message') or 'no message'}"


def _record(payload: Mapping[str, Any]) -> Record:
    record_id = str(payload["id"])
    properties = dict(payload.get("properties") or {})
    merged = str(properties.get(HS_MERGED_OBJECT_IDS) or "")
    return Record(
        id=record_id,
        merged_ids=tuple(part for part in merged.split(";") if part),
        properties=properties,
    )


class HubSpotClient:
    """Serial calls at no more than `Config.max_rps`, retried where the design says so.

    Concurrency is not needed: at the cap the design allows, serial merges take a few
    hours for the whole queue, and they cannot collide on the same primary.
    """

    def __init__(
        self,
        config: Config,
        *,
        transport: HttpTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._transport = transport or RequestsTransport()
        self._sleep = sleep
        self._clock = clock
        self._min_interval = 1.0 / config.max_rps
        self._next_call_at = 0.0

    def __repr__(self) -> str:
        # A traceback or a debug print must not put the service key in a log.
        return (
            f"HubSpotClient(base_url={self._config.hubspot_base_url!r}, "
            f"portal={self._config.expected_portal_id!r})"
        )

    def verify_portal(self) -> None:
        """Confirm the key opens the portal the configuration names, before anything writes."""
        body = self._call("GET", ACCOUNT_INFO_PATH)
        actual = str(body.get("portalId") or "")
        if actual != self._config.expected_portal_id:
            raise WrongPortalError(actual or "none", self._config.expected_portal_id)

    def batch_read(self, ids: Sequence[str], properties: Sequence[str]) -> ReadResult:
        body = self._call(
            "POST",
            BATCH_READ_PATH,
            {
                "inputs": [{"id": contact_id} for contact_id in ids],
                "properties": list(dict.fromkeys([*properties, HS_MERGED_OBJECT_IDS])),
            },
        )
        records = {record.id: record for record in map(_record, body.get("results") or [])}
        missing = tuple(
            str(contact_id)
            for error in body.get("errors") or []
            for contact_id in (error.get("context") or {}).get("ids") or []
        )
        return ReadResult(records=records, missing_ids=missing)

    def merge(self, primary_id: str, secondary_id: str) -> Record:
        """Absorb `secondary_id` into `primary_id`. The record returned is the survivor."""
        body = self._call(
            "POST",
            MERGE_PATH,
            {"primaryObjectId": primary_id, "objectIdToMerge": secondary_id},
        )
        return _record(body)

    def _call(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        url = f"{self._config.hubspot_base_url}{path}"
        headers = {"Authorization": f"Bearer {self._config.hubspot_token}"}
        retryer = Retrying(
            retry=retry_if_exception(_retryable),
            wait=_wait,
            stop=stop_after_attempt(RETRY_ATTEMPTS),
            sleep=self._sleep,
            reraise=True,
        )
        for attempt in retryer:
            with attempt:
                self._hold_the_interval()
                response = self._transport.request(
                    method, url, body=body, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS
                )
                return self._answer(response)
        raise AssertionError("unreachable: a Retrying loop always returns or raises")

    def _hold_the_interval(self) -> None:
        """Space calls at least `1 / max_rps` apart, retries included."""
        now = self._clock()
        if now < self._next_call_at:
            self._sleep(self._next_call_at - now)
            now = self._clock()
        self._next_call_at = now + self._min_interval

    @staticmethod
    def _answer(response: HttpResponse) -> Mapping[str, Any]:
        if 200 <= response.status_code < 300:
            return response.body
        if response.status_code == 429:
            raise HubSpotError(429, _error_message(response.body), _retry_after_seconds(response.headers))
        raise HubSpotError(response.status_code, _error_message(response.body))
