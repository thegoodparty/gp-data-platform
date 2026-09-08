from __future__ import annotations

import pytest
from tenacity import stop_after_attempt, wait_none

from retl.destinations import DeliveryResult, RowError
from retl.hubspot_destination import (
    HttpResponse,
    HubSpotDestination,
    HubSpotDestinationConfig,
    MissingTokenError,
    NonRetryableHubSpotError,
    RetryableHubSpotError,
    build_batch_body,
    chunked,
    config_from_env,
    parse_batch_response,
    send_batch_with_retry,
)
from tests._fakes import FakeHttpTransport


def test_chunked_splits_into_configured_batch_size_with_a_remainder() -> None:
    """Catches: a batch exceeding HubSpot's 100-record batch upsert limit."""
    rows = [(f"p{i}", "{}") for i in range(150)]
    batches = list(chunked(rows, 100))
    assert [len(batch) for batch in batches] == [100, 50]


def test_build_batch_body_sets_object_write_trace_id_to_the_tracking_key() -> None:
    """Catches: a missing objectWriteTraceId, which makes a 207 partial failure unattributable."""
    body = build_batch_body([("p1", '{"firstname":"Jane"}')])
    assert body["inputs"] == [
        {
            "idProperty": "gp_person_id",
            "id": "p1",
            "properties": {"firstname": "Jane"},
            "objectWriteTraceId": "p1",
        }
    ]


def test_config_from_env_raises_missing_token_error_when_blank() -> None:
    """Catches: an unset HubSpot token being used silently instead of failing before the first POST."""
    with pytest.raises(MissingTokenError):
        config_from_env({})


def test_parse_batch_response_confirms_using_the_original_sent_payload() -> None:
    """Catches: sent_log being written from HubSpot's echoed response instead of our own fingerprint
    (HubSpot could reformat a value, which would make sent_log stop matching our own diff)."""
    response = HttpResponse(status_code=200, body={"results": [{"objectWriteTraceId": "p1", "id": "999"}]})
    result = parse_batch_response(response, flow_id="hubspot_leads", sent_rows={"p1": '{"firstname":"Jane"}'})
    assert result.confirmed == {"p1": '{"firstname":"Jane"}'}
    assert result.errors == []


def test_parse_batch_response_extracts_row_errors_from_a_207() -> None:
    """Catches: a partial failure's rejected rows being silently dropped instead of surfaced.

    Uses HubSpot's documented error shape: a singular `objectWriteTraceId` context key
    whose value is a list.
    """
    response = HttpResponse(
        status_code=207,
        body={
            "results": [{"objectWriteTraceId": "p1"}],
            "errors": [
                {
                    "category": "VALIDATION_ERROR",
                    "context": {"objectWriteTraceId": ["p2"], "properties": ["phone"]},
                }
            ],
        },
    )
    result = parse_batch_response(
        response, flow_id="hubspot_leads", sent_rows={"p1": '{"a":1}', "p2": '{"phone":"x"}'}
    )
    assert result.confirmed == {"p1": '{"a":1}'}
    assert result.errors == [
        RowError(
            flow_id="hubspot_leads",
            tracking_key="p2",
            error_code="VALIDATION_ERROR",
            property="phone",
            retryable=False,
        )
    ]


def test_parse_batch_response_reads_a_top_level_trace_id_on_an_error() -> None:
    """Catches: dropping the top-level objectWriteTraceId fallback, in case the real sandbox
    shape puts it there instead of under context (the documented shape is unverified)."""
    response = HttpResponse(
        status_code=207,
        body={"results": [], "errors": [{"objectWriteTraceId": "p1", "category": "VALIDATION_ERROR"}]},
    )
    result = parse_batch_response(response, flow_id="hubspot_leads", sent_rows={"p1": '{"a":1}'})
    assert [error.tracking_key for error in result.errors] == ["p1"]


def test_parse_batch_response_never_confirms_a_row_hubspot_did_not_echo_back() -> None:
    """Catches: a row being marked sent when HubSpot's response never actually confirmed it."""
    response = HttpResponse(status_code=200, body={"results": []})
    result = parse_batch_response(response, flow_id="hubspot_leads", sent_rows={"p1": '{"a":1}'})
    assert result.confirmed == {}


def test_parse_batch_response_sweeps_a_canceled_body_into_unknown_delivery() -> None:
    """Catches: a whole-batch failure shaped like {"status":"CANCELED","results":[]} on HTTP
    200 producing sent=0 errors=0 -- a failed delivery day indistinguishable from a quiet one."""
    response = HttpResponse(status_code=200, body={"status": "CANCELED", "results": []})
    result = parse_batch_response(
        response, flow_id="hubspot_leads", sent_rows={"p1": '{"a":1}', "p2": '{"a":2}'}
    )
    assert result.confirmed == {}
    assert {error.tracking_key for error in result.errors} == {"p1", "p2"}
    assert {error.error_code for error in result.errors} == {"UNKNOWN_DELIVERY"}


def test_parse_batch_response_sweeps_a_result_missing_its_own_trace_id() -> None:
    """Catches: a results entry that omits objectWriteTraceId leaving its input neither
    confirmed nor surfaced as an error."""
    response = HttpResponse(status_code=200, body={"results": [{"id": "999"}]})
    result = parse_batch_response(response, flow_id="hubspot_leads", sent_rows={"p1": '{"a":1}'})
    assert result.confirmed == {}
    assert result.errors == [
        RowError(
            flow_id="hubspot_leads",
            tracking_key="p1",
            error_code="UNKNOWN_DELIVERY",
            property=None,
            retryable=False,
        )
    ]


def test_send_batch_with_retry_retries_a_retryable_status_then_succeeds() -> None:
    """Catches: a transient 503 failing the run instead of being retried."""
    transport = FakeHttpTransport(responses=[HttpResponse(503, {}), HttpResponse(200, {"results": []})])
    response = send_batch_with_retry(
        transport,
        url="https://api.hubapi.com/x",
        body={},
        headers={},
        timeout=1.0,
        wait=wait_none(),
        stop=stop_after_attempt(3),
    )
    assert response.status_code == 200
    assert len(transport.calls) == 2


def test_send_batch_with_retry_treats_an_unlisted_status_as_non_retryable() -> None:
    """Catches: assuming any 5xx is retryable and looping on a bare 500, which is not in the allowlist."""
    transport = FakeHttpTransport(responses=[HttpResponse(500, {}), HttpResponse(200, {"results": []})])
    with pytest.raises(NonRetryableHubSpotError):
        send_batch_with_retry(
            transport,
            url="https://api.hubapi.com/x",
            body={},
            headers={},
            timeout=1.0,
            wait=wait_none(),
            stop=stop_after_attempt(3),
        )
    assert len(transport.calls) == 1


def test_send_batch_with_retry_raises_non_retryable_without_retrying() -> None:
    """Catches: retrying a 401, which would keep hammering an authentication failure instead of failing loud."""
    transport = FakeHttpTransport(responses=[HttpResponse(401, {}), HttpResponse(200, {"results": []})])
    with pytest.raises(NonRetryableHubSpotError):
        send_batch_with_retry(
            transport,
            url="https://api.hubapi.com/x",
            body={},
            headers={},
            timeout=1.0,
            wait=wait_none(),
            stop=stop_after_attempt(3),
        )
    assert len(transport.calls) == 1


def test_send_batch_with_retry_raises_after_exhausting_attempts() -> None:
    """Catches: a persistently-failing retryable call looping forever instead of eventually failing the run."""
    transport = FakeHttpTransport(responses=[HttpResponse(503, {}), HttpResponse(503, {})])
    with pytest.raises(RetryableHubSpotError):
        send_batch_with_retry(
            transport,
            url="https://api.hubapi.com/x",
            body={},
            headers={},
            timeout=1.0,
            wait=wait_none(),
            stop=stop_after_attempt(2),
        )
    assert len(transport.calls) == 2


def test_hubspot_destination_confirms_each_batch_before_the_next_batch_posts() -> None:
    """Catches: per-batch confirm CALLS existing but not actually firing before the next
    batch's POST -- asserting only call count and sizes after delivery returns cannot tell
    that apart from an end-of-run append with per-batch granularity. This is the exact
    invariant per-batch logging exists for: a later batch's send must never be able to
    happen before an earlier batch's confirm does, or a later-batch failure would strand
    the earlier batch's confirmations unlogged."""
    rows = [(f"p{i}", f'{{"n":{i}}}') for i in range(150)]  # 2 batches: 100 + 50
    transport = FakeHttpTransport(
        responses=[
            HttpResponse(200, {"results": [{"objectWriteTraceId": f"p{i}"} for i in range(100)]}),
            HttpResponse(200, {"results": [{"objectWriteTraceId": f"p{i}"} for i in range(100, 150)]}),
        ]
    )
    destination = HubSpotDestination(
        HubSpotDestinationConfig(base_url="https://api.hubapi.com", token="secret"), transport=transport
    )
    confirmed_calls: list[dict[str, str]] = []
    posts_seen_at_confirm: list[int] = []

    def _on_batch_confirmed(confirmed: dict[str, str]) -> None:
        confirmed_calls.append(confirmed)
        posts_seen_at_confirm.append(len(transport.calls))

    delivery = destination.deliver("hubspot_leads", rows, on_batch_confirmed=_on_batch_confirmed)

    assert [len(c) for c in confirmed_calls] == [100, 50]
    assert posts_seen_at_confirm == [1, 2]  # first confirm saw exactly 1 POST made, not 2
    assert isinstance(delivery, DeliveryResult)
    assert len(delivery.confirmed) == 150


def test_hubspot_destination_confirms_the_first_batch_before_a_later_batch_raises() -> None:
    """Catches: a refactor moving on_batch_confirmed after the loop, which would strand the
    first batch's confirmations unlogged when a later batch fails -- the whole reason
    per-batch appends exist."""
    rows = [(f"p{i}", f'{{"n":{i}}}') for i in range(150)]  # 2 batches: 100 + 50
    transport = FakeHttpTransport(
        responses=[
            HttpResponse(200, {"results": [{"objectWriteTraceId": f"p{i}"} for i in range(100)]}),
            HttpResponse(401, {}),
        ]
    )
    destination = HubSpotDestination(
        HubSpotDestinationConfig(base_url="https://api.hubapi.com", token="secret"), transport=transport
    )
    confirmed_calls: list[dict[str, str]] = []

    with pytest.raises(NonRetryableHubSpotError):
        destination.deliver("hubspot_leads", rows, on_batch_confirmed=confirmed_calls.append)

    assert len(confirmed_calls) == 1
    assert len(confirmed_calls[0]) == 100


def test_hubspot_destination_never_puts_the_token_in_the_request_body() -> None:
    """Catches: the token leaking into the payload/body instead of staying only in the Authorization header."""
    transport = FakeHttpTransport(responses=[HttpResponse(200, {"results": [{"objectWriteTraceId": "p1"}]})])
    destination = HubSpotDestination(
        HubSpotDestinationConfig(base_url="https://api.hubapi.com", token="super-secret"), transport=transport
    )
    destination.deliver("hubspot_leads", [("p1", '{"a":1}')], on_batch_confirmed=lambda _confirmed: None)

    assert "super-secret" not in str(transport.calls[0]["json"])
    assert transport.calls[0]["headers"]["Authorization"] == "Bearer super-secret"
