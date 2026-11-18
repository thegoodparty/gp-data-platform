from __future__ import annotations

import pytest

from hsmerge.client import (
    ACCOUNT_INFO_PATH,
    HS_MERGED_OBJECT_IDS,
    MERGE_PATH,
    HttpResponse,
    HubSpotClient,
    HubSpotError,
    WrongPortalError,
)
from hsmerge.config import Config
from tests._fakes import (
    ABSENT_ID,
    ABSENT_READ,
    ACCOUNT_INFO,
    FORWARD_REFERENCE_400,
    LIVE_RECORD,
    MERGED_RECORD,
    READ_ENVELOPE,
    SURVIVOR_RECORD,
    FakeClock,
    FakeTransport,
    json_headers,
)

TOKEN = "pat-na1-test-token"
PRIMARY = SURVIVOR_RECORD["id"]
SECONDARY = LIVE_RECORD["id"]


def config(**overrides: object) -> Config:
    values = {
        "hubspot_token": TOKEN,
        "hubspot_base_url": "https://api.hubapi.test",
        "expected_portal_id": "51780263",
        "object_type": "contacts",
        "log_table": "catalog.airflow_source_dev.hubspot_contact_merge_log",
        "max_rps": 2.0,
        "databricks_host": "https://example.cloud.databricks.com",
        "databricks_http_path": "/sql/1.0/warehouses/abc",
        "databricks_client_id": "client-id",
        "databricks_client_secret": "client-secret",
    }
    return Config(**{**values, **overrides})  # type: ignore[arg-type]


def client(transport: FakeTransport, clock: FakeClock | None = None, **overrides: object) -> HubSpotClient:
    clock = clock or FakeClock()
    return HubSpotClient(config(**overrides), transport=transport, sleep=clock.sleep, clock=clock.time)


def test_portal_mismatch_refuses_before_any_call() -> None:
    """Catches: a production key in the sandbox variable running merges against real contacts.
    A service key carries no portal in its text, so only this call can tell."""
    transport = FakeTransport([_ok({**ACCOUNT_INFO, "portalId": 21589597})])

    with pytest.raises(WrongPortalError, match="21589597"):
        client(transport).verify_portal()

    assert [call["url"] for call in transport.calls] == ["https://api.hubapi.test" + ACCOUNT_INFO_PATH]


def test_429_honours_retry_after_and_then_succeeds() -> None:
    """Catches: giving up on a rate limit, and ignoring the pause HubSpot asked for."""
    clock = FakeClock()
    transport = FakeTransport(
        [
            _response(429, {}, retry_after="7"),
            _ok({**READ_ENVELOPE, "results": [SURVIVOR_RECORD, LIVE_RECORD]}),
        ]
    )

    result = client(transport, clock, max_rps=1000.0).batch_read([PRIMARY, SECONDARY], ["email"])

    assert sorted(result.records) == sorted([PRIMARY, SECONDARY])
    assert clock.sleeps == [7.0]
    assert len(transport.calls) == 2


def test_400_does_not_retry() -> None:
    """Catches: retrying a refusal. A merge on a secondary that is already merged away
    answers 400 with a forward reference, and retrying it changes nothing."""
    transport = FakeTransport([_response(400, FORWARD_REFERENCE_400)])

    with pytest.raises(HubSpotError) as raised:
        client(transport).merge(PRIMARY, SECONDARY)

    assert raised.value.status_code == 400
    assert "forward reference" in str(raised.value)
    assert len(transport.calls) == 1


def test_a_second_call_waits_the_minimum_interval() -> None:
    """Catches: a client that ignores max_rps and bursts past the portal's limit."""
    clock = FakeClock()
    transport = FakeTransport([_ok(ACCOUNT_INFO)])

    hubspot = client(transport, clock, max_rps=2.0)
    hubspot.verify_portal()
    hubspot.verify_portal()

    assert clock.sleeps == [0.5]


def test_a_batch_read_attributes_records_by_id_and_names_the_merged_ones() -> None:
    """Catches: pairing a response to the pair that asked for it by position. The read
    answers one record per resolved object, in no order, with no echo of the ids asked."""
    transport = FakeTransport([_ok({**READ_ENVELOPE, "results": [SURVIVOR_RECORD]})])

    result = client(transport).batch_read([PRIMARY, SECONDARY], ["email"])

    assert list(result.records) == [PRIMARY]
    assert result.records[PRIMARY].merged_ids == (PRIMARY, SECONDARY)
    assert result.missing_ids == ()
    assert transport.calls[0]["body"]["properties"] == ["email", HS_MERGED_OBJECT_IDS]


def test_resolved_folds_an_absorbed_id_onto_the_record_that_answered_for_it() -> None:
    """The map classification reads. Catches: an absorbed id going unmentioned, since the
    response names it only on the survivor's record and never returns a record of its own."""
    transport = FakeTransport([_ok({**READ_ENVELOPE, "results": [SURVIVOR_RECORD]})])

    result = client(transport).batch_read([PRIMARY, SECONDARY], ["email"])

    assert result.resolved == {PRIMARY: PRIMARY, SECONDARY: PRIMARY}


def test_resolved_keeps_a_never_merged_record_speaking_only_for_itself() -> None:
    """Catches: reading HubSpot's null hs_merged_object_ids as an id, which would make a
    live contact appear to answer for another one."""
    transport = FakeTransport([_ok({**READ_ENVELOPE, "results": [LIVE_RECORD]})])

    result = client(transport).batch_read([SECONDARY], ["email"])

    assert result.resolved == {SECONDARY: SECONDARY}
    assert result.records[SECONDARY].merged_ids == ()


def test_a_batch_read_names_the_ids_hubspot_could_not_find() -> None:
    """Catches: reading an absent id as a pair that resolved to nothing it can name."""
    transport = FakeTransport([_ok(ABSENT_READ)])

    result = client(transport).batch_read([ABSENT_ID], ["email"])

    assert result.records == {}
    assert result.missing_ids == (ABSENT_ID,)


def test_a_merge_returns_the_survivors_record() -> None:
    transport = FakeTransport([_ok(MERGED_RECORD)])

    survivor = client(transport).merge(PRIMARY, SECONDARY)

    assert survivor.id == PRIMARY
    assert survivor.merged_ids == (PRIMARY, SECONDARY)
    assert transport.calls[0]["body"] == {"primaryObjectId": PRIMARY, "objectIdToMerge": SECONDARY}
    assert transport.calls[0]["url"] == "https://api.hubapi.test" + MERGE_PATH


def test_the_token_reaches_no_repr_and_no_error_message() -> None:
    """Catches: a traceback or a log line carrying the service key."""
    transport = FakeTransport([_response(400, FORWARD_REFERENCE_400)])
    hubspot = client(transport)

    assert TOKEN not in repr(hubspot)
    with pytest.raises(HubSpotError) as raised:
        hubspot.merge(PRIMARY, SECONDARY)
    assert TOKEN not in str(raised.value)
    assert transport.calls[0]["headers"]["Authorization"] == f"Bearer {TOKEN}"


def _ok(body: dict) -> HttpResponse:
    return _response(200, body)


def _response(status_code: int, body: dict, retry_after: str | None = None) -> HttpResponse:
    headers = json_headers() if retry_after is None else json_headers(**{"Retry-After": retry_after})
    return HttpResponse(status_code=status_code, body=body, headers=headers)
