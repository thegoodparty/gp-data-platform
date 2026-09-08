"""The HubSpot destination: v3 batch upsert, keyed on the `gp_person_id` property.

Batches of 100. Every input carries `objectWriteTraceId` set to its own
tracking_key: HubSpot's batch `id` in a response is its OWN internal object id,
never the idProperty value we upserted on, so objectWriteTraceId is the only way to
attribute a result -- success or error -- back to the row that produced it. A 207
partial failure is otherwise unattributable.

The exact shape of a 207 partial-failure body has not been exercised against a
live HubSpot sandbox yet. `parse_batch_response` reads the documented, stable
outer envelope (`results` / `errors` / status code) and degrades to an
unattributed error rather than raising when a single error entry does not carry
enough to name a tracking_key or property.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_exponential

from .destinations import DeliveryResult, OnBatchConfirmed, RowError

HUBSPOT_ID_PROPERTY = "gp_person_id"
BATCH_SIZE = 100
UPSERT_PATH = "/crm/v3/objects/contacts/batch/upsert"

# Doc-verified retry allowlist. 401/403/414 are explicitly non-retryable; anything
# else outside this set is ALSO treated as non-retryable by _classify_response's
# fallthrough, on the conservative assumption that an unenumerated failure needs a
# human, not a resend.
RETRYABLE_STATUS_CODES = frozenset({429, 423, 477, 502, 503, 504, 523, 524})


class MissingTokenError(ValueError):
    def __init__(self, env_var: str):
        super().__init__(f"{env_var} is not set")


class NonRetryableHubSpotError(RuntimeError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"HubSpot batch call failed with non-retryable status {status_code}")


class RetryableHubSpotError(RuntimeError):
    def __init__(self, status_code: int):
        self.status_code = status_code
        super().__init__(f"HubSpot batch call failed with retryable status {status_code}")


@dataclass(frozen=True)
class HttpResponse:
    status_code: int
    body: dict[str, Any]


class HttpTransport(Protocol):
    def post(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: float
    ) -> HttpResponse: ...


class RequestsTransport:
    """The real transport. Not unit tested: it is the one call that must reach HubSpot."""

    def post(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str], timeout: float
    ) -> HttpResponse:
        import requests

        response = requests.post(url, json=json, headers=headers, timeout=timeout)
        try:
            body = response.json()
        except ValueError:
            body = {}
        return HttpResponse(status_code=response.status_code, body=body)


@dataclass(frozen=True)
class HubSpotDestinationConfig:
    base_url: str
    token: str
    request_timeout: float = 30.0


def config_from_env(env: Mapping[str, str]) -> HubSpotDestinationConfig:
    """Build config from RETL_HUBSPOT_*. The token needs crm.objects.contacts.write
    (batch upsert); confirm the exact scope list against the sandbox private app
    before enable."""
    token = env.get("RETL_HUBSPOT_TOKEN", "")
    if not token:
        raise MissingTokenError("RETL_HUBSPOT_TOKEN")
    return HubSpotDestinationConfig(
        base_url=env.get("RETL_HUBSPOT_BASE_URL", "https://api.hubapi.com"),
        token=token,
    )


def chunked(rows: Sequence[tuple[str, str]], size: int = BATCH_SIZE) -> Iterator[Sequence[tuple[str, str]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def build_batch_body(batch: Sequence[tuple[str, str]]) -> dict[str, Any]:
    """One batch's request body. `id` and `objectWriteTraceId` are both the tracking_key."""
    return {
        "inputs": [
            {
                "idProperty": HUBSPOT_ID_PROPERTY,
                "id": tracking_key,
                "properties": json.loads(payload),
                "objectWriteTraceId": tracking_key,
            }
            for tracking_key, payload in batch
        ]
    }


def _classify_response(response: HttpResponse) -> HttpResponse:
    if response.status_code in (200, 207):
        return response
    if response.status_code in RETRYABLE_STATUS_CODES:
        raise RetryableHubSpotError(response.status_code)
    raise NonRetryableHubSpotError(response.status_code)


def send_batch_with_retry(
    transport: HttpTransport,
    *,
    url: str,
    body: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
    wait: Any = None,
    stop: Any = None,
) -> HttpResponse:
    """POST once; retry only on RETRYABLE_STATUS_CODES, with backoff. Anything else raises immediately."""
    retryer = Retrying(
        retry=retry_if_exception_type(RetryableHubSpotError),
        wait=wait if wait is not None else wait_exponential(multiplier=1, max=30),
        stop=stop if stop is not None else stop_after_attempt(5),
        reraise=True,
    )
    for attempt in retryer:
        with attempt:
            response = transport.post(url, json=body, headers=headers, timeout=timeout)
            return _classify_response(response)
    raise AssertionError("unreachable: Retrying always returns or raises")


def _error_tracking_key(error: Mapping[str, Any]) -> str | None:
    trace_id = error.get("objectWriteTraceId")
    if trace_id:
        return str(trace_id)
    context = error.get("context") or {}
    # HubSpot's documented error envelope carries the singular key with a list value;
    # the plural spelling is an unverified fallback until the sandbox settles the real
    # shape -- read both rather than betting on one.
    trace_ids = context.get("objectWriteTraceId") or context.get("objectWriteTraceIds") or []
    return str(trace_ids[0]) if trace_ids else None


def _error_property(error: Mapping[str, Any]) -> str | None:
    context = error.get("context") or {}
    properties = context.get("properties") or context.get("propertyName")
    if isinstance(properties, list) and properties:
        return str(properties[0])
    if isinstance(properties, str) and properties:
        return properties
    return None


UNKNOWN_DELIVERY_CODE = "UNKNOWN_DELIVERY"


def parse_batch_response(
    response: HttpResponse, *, flow_id: str, sent_rows: Mapping[str, str]
) -> DeliveryResult:
    """One batch's outcome, from its HTTP response and the payloads that batch sent.

    Confirmed payloads are always the ORIGINAL serialized payloads from `sent_rows`
    (looked up by objectWriteTraceId), never anything reconstructed from HubSpot's
    response body, so sent_log always holds exactly what our own diff produced.

    Every input in `sent_rows` must come out of this function either confirmed or
    named by an error. Without that sweep, a body shaped like
    `{"status": "CANCELED", "results": []}` on HTTP 200 -- or a results entry that
    omits its own objectWriteTraceId -- would leave those inputs both unconfirmed
    and unreported: a failed delivery day indistinguishable from a quiet one. An
    input left over after confirmed rows and attributable errors are removed gets a
    manufactured UNKNOWN_DELIVERY error instead. This can double-count a row that
    already has an unattributable error (tracking_key None) of its own; that is
    accepted, since over-signal beats silence and an unlogged row still retries in
    tomorrow's diff either way.
    """
    confirmed: dict[str, str] = {}
    for result in response.body.get("results", []):
        trace_id = result.get("objectWriteTraceId")
        if trace_id in sent_rows:
            confirmed[trace_id] = sent_rows[trace_id]

    errors = [
        RowError(
            flow_id=flow_id,
            tracking_key=_error_tracking_key(error),
            error_code=str(error.get("category") or error.get("status") or "UNKNOWN"),
            property=_error_property(error),
            retryable=False,  # a row rejected inside a 200/207 needs a data fix, not a resend
        )
        for error in response.body.get("errors", [])
    ]

    accounted_for = set(confirmed) | {e.tracking_key for e in errors if e.tracking_key is not None}
    errors.extend(
        RowError(
            flow_id=flow_id,
            tracking_key=tracking_key,
            error_code=UNKNOWN_DELIVERY_CODE,
            property=None,
            retryable=False,
        )
        for tracking_key in sent_rows
        if tracking_key not in accounted_for
    )

    return DeliveryResult(confirmed=confirmed, errors=errors)


class HubSpotDestination:
    def __init__(self, config: HubSpotDestinationConfig, *, transport: HttpTransport | None = None):
        self._config = config
        self._transport = transport or RequestsTransport()

    def deliver(
        self,
        flow_id: str,
        rows: Sequence[tuple[str, str]],
        *,
        on_batch_confirmed: OnBatchConfirmed,
    ) -> DeliveryResult:
        url = f"{self._config.base_url}{UPSERT_PATH}"
        headers = {"Authorization": f"Bearer {self._config.token}"}

        all_confirmed: dict[str, str] = {}
        all_errors: list[RowError] = []
        for batch in chunked(rows):
            sent_rows = dict(batch)
            response = send_batch_with_retry(
                self._transport,
                url=url,
                body=build_batch_body(batch),
                headers=headers,
                timeout=self._config.request_timeout,
            )
            result = parse_batch_response(response, flow_id=flow_id, sent_rows=sent_rows)
            if result.confirmed:
                on_batch_confirmed(result.confirmed)  # log THIS batch now, before the next one runs
            all_confirmed.update(result.confirmed)
            all_errors.extend(result.errors)

        return DeliveryResult(confirmed=all_confirmed, errors=all_errors)
