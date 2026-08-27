import base64
import json
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
import requests
from include.custom_functions.ballotready_graphql import (
    CANDIDACY_SELECTION,
    ENDORSEMENT_SELECTION,
    ENTITY_SPECS,
    FILING_PERIOD_SELECTION,
    GEOFENCE_SELECTION,
    INSERT_BATCH_SIZE,
    ISSUE_SELECTION,
    NORMALIZED_POSITION_SELECTION,
    PARTY_SELECTION,
    POSITION_ELECTION_FREQUENCY_SELECTION,
    STANCE_SELECTION,
    WINDOW_SIZE,
    EntitySpec,
    ExtractConfig,
    FetchedNode,
    RateLimiter,
    build_insert_rows,
    candidacy_worklist_sql,
    chunked,
    create_landing_table,
    encode_node_id,
    extract_entity,
    fetch_nodes,
    geofence_worklist_sql,
    insert_rows,
    is_retryable_status,
    issue_worklist_sql,
    landing_table,
    make_session,
    position_derived_worklist_sql,
    race_derived_worklist_sql,
    read_cursor,
    retry_wait_seconds,
    validate_identifier,
)


def test_encode_node_id_uses_the_ballot_factory_global_id_format():
    encoded = encode_node_id("Candidacy", 12345)
    assert base64.b64decode(encoded).decode() == "gid://ballot-factory/Candidacy/12345"


def test_encode_node_id_varies_by_node_type():
    assert encode_node_id("Issue", 7) != encode_node_id("Geofence", 7)


def test_chunked_splits_into_fixed_size_batches():
    assert list(chunked([1, 2, 3, 4, 5], 2)) == [[1, 2], [3, 4], [5]]


def test_chunked_rejects_a_size_below_one():
    with pytest.raises(ValueError, match="chunk size"):
        list(chunked([1, 2], 0))


class FakeClock:
    """A monotonic clock that only advances when sleep() is called."""

    def __init__(self):
        self.now = 0.0
        self.slept = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def test_rate_limiter_spaces_calls_by_the_configured_interval():
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=2.0, sleep=clock.sleep, clock=clock.time)

    limiter.acquire()  # first call is free
    limiter.acquire()  # must wait 0.5s

    assert clock.slept == [pytest.approx(0.5)]


def test_rate_limiter_does_not_sleep_when_enough_time_has_passed():
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=2.0, sleep=clock.sleep, clock=clock.time)

    limiter.acquire()
    clock.now += 10.0
    limiter.acquire()

    assert clock.slept == []


def test_pause_for_holds_every_subsequent_acquire():
    """A 429 seen by one worker must stop all of them, not just the one that saw it."""
    clock = FakeClock()
    limiter = RateLimiter(requests_per_second=1000.0, sleep=clock.sleep, clock=clock.time)

    limiter.pause_for(30.0)
    limiter.acquire()

    assert clock.slept and clock.slept[0] == pytest.approx(30.0)


def test_rate_limiter_rejects_a_non_positive_rate():
    with pytest.raises(ValueError, match="requests_per_second"):
        RateLimiter(requests_per_second=0)


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_retryable_statuses(status):
    assert is_retryable_status(status)


@pytest.mark.parametrize("status", [200, 400, 401, 403, 404, 422])
def test_non_retryable_statuses(status):
    assert not is_retryable_status(status)


def test_retry_wait_honors_a_well_formed_retry_after_header():
    assert retry_wait_seconds({"Retry-After": "12"}, attempt=0) == 12.0


def test_retry_wait_reads_retry_after_case_insensitively():
    assert retry_wait_seconds({"retry-after": "5"}, attempt=0) == 5.0


def test_retry_wait_caps_retry_after_at_max_backoff():
    assert retry_wait_seconds({"Retry-After": "9999"}, attempt=0, max_backoff=60.0) == 60.0


def test_retry_wait_falls_back_to_jittered_backoff_when_retry_after_is_garbage():
    # rng returns its upper bound so the growth is assertable.
    wait = retry_wait_seconds({"Retry-After": "soon"}, attempt=2, base_backoff=1.0, rng=lambda lo, hi: hi)
    assert wait == 4.0


def test_retry_wait_backoff_grows_with_the_attempt_number():
    rng = lambda lo, hi: hi  # noqa: E731
    waits = [retry_wait_seconds({}, attempt=n, base_backoff=1.0, rng=rng) for n in range(4)]
    assert waits == [1.0, 2.0, 4.0, 8.0]


def test_retry_wait_never_returns_a_negative_wait_for_a_negative_retry_after():
    wait = retry_wait_seconds({"Retry-After": "-5"}, attempt=0)
    assert wait >= 0.0


SELECTION = "... on Candidacy { databaseId id }"


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Returns queued responses in order and records the ids each call requested.

    A queued item that is an Exception instance is raised instead of returned,
    so a caller can simulate a network-level failure (ConnectionError, Timeout, ...)
    on a given call.
    """

    def __init__(self, responses):
        self._responses = list(responses)
        self.requested_id_counts = []
        self.requested_ids = []

    def post(self, url, json, headers, timeout):
        self.requested_id_counts.append(len(json["variables"]["ids"]))
        self.requested_ids.append(json["variables"]["ids"])
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _ok(nodes):
    return FakeResponse(body={"data": {"nodes": nodes}})


def _limiter():
    return RateLimiter(requests_per_second=1000.0, sleep=lambda s: None, clock=lambda: 0.0)


def test_fetch_nodes_maps_results_positionally():
    session = FakeSession([_ok([{"databaseId": 11}, None, {"databaseId": 33}])])

    result = fetch_nodes([1, 2, 3], "Candidacy", SELECTION, "tok", _limiter(), session)

    assert result == [
        FetchedNode(requested_id=1, node={"databaseId": 11}),
        FetchedNode(requested_id=2, node=None),
        FetchedNode(requested_id=3, node={"databaseId": 33}),
    ]


def test_fetch_nodes_does_not_key_results_by_database_id():
    """databaseId need not equal the requested id, so position is the only safe mapping."""
    session = FakeSession([_ok([{"databaseId": 999}])])

    result = fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session)

    assert result == [FetchedNode(requested_id=1, node={"databaseId": 999})]


def test_fetch_nodes_bisects_when_the_response_is_short():
    """A short array means silent truncation, so split and retry rather than lose rows."""
    session = FakeSession(
        [
            _ok([{"databaseId": 1}, {"databaseId": 2}]),  # 4 requested, 2 returned
            _ok([{"databaseId": 1}, {"databaseId": 2}]),  # first half
            _ok([{"databaseId": 3}, {"databaseId": 4}]),  # second half
        ]
    )

    result = fetch_nodes([1, 2, 3, 4], "Candidacy", SELECTION, "tok", _limiter(), session)

    assert [r.requested_id for r in result] == [1, 2, 3, 4]
    assert session.requested_id_counts == [4, 2, 2]
    # Pins which ids each recursive call actually sent, not just how many, so a
    # shuffled argument order in the recursive call (e.g. node_type swapped with
    # another positional arg) fails here instead of silently fetching wrong ids.
    assert session.requested_ids == [
        [encode_node_id("Candidacy", i) for i in [1, 2, 3, 4]],
        [encode_node_id("Candidacy", i) for i in [1, 2]],
        [encode_node_id("Candidacy", i) for i in [3, 4]],
    ]


def test_fetch_nodes_raises_when_a_single_id_request_is_still_short():
    session = FakeSession([_ok([])])

    with pytest.raises(RuntimeError, match="returned 0 nodes for 1 id"):
        fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session)


def test_fetch_nodes_retries_a_429_and_pauses_every_worker():
    limiter = _limiter()
    paused = []
    limiter.pause_for = lambda s: paused.append(s)
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "7"}), _ok([{"databaseId": 1}])])

    result = fetch_nodes([1], "Candidacy", SELECTION, "tok", limiter, session, sleep=lambda s: None)

    assert paused == [7.0]
    assert result == [FetchedNode(requested_id=1, node={"databaseId": 1})]


def test_fetch_nodes_retries_a_connection_error_then_succeeds():
    session = FakeSession([requests.exceptions.ConnectionError("boom"), _ok([{"databaseId": 1}])])

    result = fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session, sleep=lambda s: None)

    assert result == [FetchedNode(requested_id=1, node={"databaseId": 1})]


def test_fetch_nodes_reraises_a_persistent_request_exception_after_max_retries():
    session = FakeSession([requests.exceptions.ConnectionError("boom")] * 3)

    with pytest.raises(requests.exceptions.ConnectionError):
        fetch_nodes(
            [1], "Candidacy", SELECTION, "tok", _limiter(), session, max_retries=2, sleep=lambda s: None
        )


def test_fetch_nodes_sleeps_between_network_exception_retries():
    """A retry must back off through the injected sleep, not hot-loop straight to the next attempt."""
    slept = []
    session = FakeSession([requests.exceptions.ConnectionError("boom"), _ok([{"databaseId": 1}])])

    fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session, sleep=slept.append)

    assert len(slept) == 1
    assert slept[0] >= 0.0


def test_fetch_nodes_raises_on_graphql_errors():
    session = FakeSession([FakeResponse(body={"errors": [{"message": "nope"}]})])

    with pytest.raises(RuntimeError, match="CivicEngine GraphQL errors"):
        fetch_nodes([1], "Candidacy", SELECTION, "tok", _limiter(), session)


def test_fetch_nodes_sends_encoded_ids_and_a_bearer_token():
    captured = {}

    class CapturingSession(FakeSession):
        def post(self, url, json, headers, timeout):
            captured["url"] = url
            captured["ids"] = json["variables"]["ids"]
            captured["auth"] = headers["Authorization"]
            return super().post(url, json, headers, timeout)

    session = CapturingSession([_ok([{"databaseId": 1}])])
    fetch_nodes([42], "Issue", SELECTION, "tok", _limiter(), session)

    assert captured["url"] == "https://bpi.civicengine.com/graphql"
    assert captured["ids"] == [encode_node_id("Issue", 42)]
    assert captured["auth"] == "Bearer tok"


ALL_SELECTIONS = {
    "Candidacy": CANDIDACY_SELECTION,
    "Endorsement": ENDORSEMENT_SELECTION,
    "FilingPeriod": FILING_PERIOD_SELECTION,
    "Geofence": GEOFENCE_SELECTION,
    "Issue": ISSUE_SELECTION,
    "NormalizedPosition": NORMALIZED_POSITION_SELECTION,
    "Party": PARTY_SELECTION,
    "PositionElectionFrequency": POSITION_ELECTION_FREQUENCY_SELECTION,
    "Stance": STANCE_SELECTION,
}


@pytest.mark.parametrize("name,selection", sorted(ALL_SELECTIONS.items()))
def test_every_selection_is_an_inline_fragment_with_balanced_braces(name, selection):
    assert selection.strip().startswith("... on "), f"{name} is not an inline fragment"
    assert selection.count("{") == selection.count("}"), f"{name} has unbalanced braces"


@pytest.mark.parametrize("name,selection", sorted(ALL_SELECTIONS.items()))
def test_no_selection_carries_the_outer_query_wrapper(name, selection):
    # _build_query supplies `query GetNodesBatch(...) { nodes(ids: $ids) { ... } }`.
    assert "query Get" not in selection, f"{name} still has the query wrapper"
    assert "nodes(ids:" not in selection, f"{name} still has the nodes() wrapper"


def _top_level_fields(selection: str) -> frozenset[str]:
    """Names of the fields directly inside the `... on Type { ... }` body.

    Ignores commented-out lines (`#...`) and anything nested inside a field's
    own `{ ... }` sub-selection, so a dropped or added top-level field is
    caught but renaming something two levels deep is not.
    """
    live = "\n".join(line for line in selection.splitlines() if not line.strip().startswith("#"))
    body = live[live.index("{") + 1 : live.rindex("}")]

    fields = []
    depth = 0
    i = 0
    while i < len(body):
        char = body[i]
        if char == "{":
            depth += 1
            i += 1
        elif char == "}":
            depth -= 1
            i += 1
        elif char.isalnum() or char == "_":
            j = i
            while j < len(body) and (body[j].isalnum() or body[j] == "_"):
                j += 1
            if depth == 0:
                fields.append(body[i:j])
            i = j
        else:
            i += 1
    return frozenset(fields)


# Complete top-level field set for each selection, verified against the dbt
# source it was copied from. A dropped, renamed, or newly added top-level
# field fails this even if it is buried among a dozen others.
EXPECTED_TOP_LEVEL_FIELDS = {
    "Candidacy": frozenset(
        {
            "candidate",
            "createdAt",
            "databaseId",
            "election",
            "endorsements",
            "id",
            "isCertified",
            "isHidden",
            "parties",
            "position",
            "race",
            "result",
            "stances",
            "updatedAt",
            "withdrawn",
        }
    ),
    "Endorsement": frozenset({"id", "databaseId", "endorsements"}),
    "FilingPeriod": frozenset(
        {"createdAt", "databaseId", "endOn", "id", "notes", "startOn", "type", "updatedAt"}
    ),
    "Geofence": frozenset(
        {"createdAt", "databaseId", "geoId", "id", "mtfcc", "updatedAt", "validFrom", "validTo"}
    ),
    "Issue": frozenset({"databaseId", "id", "key", "name", "pluginEnabled", "responseType", "rowOrder"}),
    "NormalizedPosition": frozenset({"databaseId", "description", "id", "issues", "mtfcc", "name"}),
    "Party": frozenset({"id", "databaseId", "parties"}),
    "PositionElectionFrequency": frozenset(
        {"databaseId", "frequency", "id", "referenceYear", "seats", "validFrom", "validTo"}
    ),
    "Stance": frozenset({"id", "databaseId", "stances"}),
}


@pytest.mark.parametrize("name,selection", sorted(ALL_SELECTIONS.items()))
def test_selection_top_level_fields_match_the_replaced_model_exactly(name, selection):
    assert _top_level_fields(selection) == EXPECTED_TOP_LEVEL_FIELDS[name]


def test_stance_selection_keeps_the_nested_issue_reference():
    # The issue worklist reads issue ids back out of landed stance payloads.
    assert "stances" in STANCE_SELECTION
    assert "issue" in STANCE_SELECTION


def test_entity_spec_is_frozen():
    spec = EntitySpec("issue", "Issue", ISSUE_SELECTION, 100, lambda **kwargs: "")
    with pytest.raises(FrozenInstanceError):
        spec.name = "other"


def test_entity_spec_carries_its_fields():
    builder = lambda **kwargs: "sql"  # noqa: E731
    spec = EntitySpec("issue", "Issue", ISSUE_SELECTION, 100, builder)
    assert (spec.name, spec.node_type, spec.batch_size) == ("issue", "Issue", 100)
    assert spec.worklist_sql is builder


def test_entity_spec_reads_tables_defaults_to_empty():
    spec = EntitySpec("issue", "Issue", ISSUE_SELECTION, 100, lambda **kwargs: "")
    assert spec.reads_tables == ()


def test_landing_table_is_backtick_qualified():
    assert landing_table("cat", "sch", "issue") == "`cat`.`sch`.`ballotready_issue_raw`"


def test_landing_table_names_follow_the_entity():
    assert landing_table("c", "s", "position_election_frequency").endswith(
        "`ballotready_position_election_frequency_raw`"
    )


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def execute(self, sql, parameters=None):
        self.executed.append((sql, parameters))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConnection:
    def __init__(self, rows):
        self._cursor = FakeCursor(rows)

    def cursor(self, *args, **kwargs):
        return self._cursor


def test_validate_identifier_accepts_a_plain_name():
    assert validate_identifier("catalog", "goodparty_data_catalog") == "goodparty_data_catalog"


@pytest.mark.parametrize("bad", ["has space", "has-dash", "has`tick", "drop;table", ""])
def test_validate_identifier_rejects_anything_that_is_not_a_bare_identifier(bad):
    with pytest.raises(ValueError, match="catalog"):
        validate_identifier("catalog", bad)


def test_read_cursor_returns_none_when_the_landing_table_is_empty():
    assert read_cursor(FakeConnection([]), "cat", "sch", "issue") == (None, None)


def test_read_cursor_returns_the_highest_landed_pair():
    landed = datetime(2026, 8, 1, 12, 0, 0)
    assert read_cursor(FakeConnection([(landed, 99)]), "cat", "sch", "issue") == (landed, 99)


def test_worklist_orders_by_the_keyset_pair_and_applies_the_limit():
    sql = candidacy_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=500)
    assert "ORDER BY source_changed_at ASC, source_id ASC" in sql
    assert "LIMIT 500" in sql


def test_worklist_emits_a_keyset_predicate_when_a_cursor_is_present():
    sql = candidacy_worklist_sql(
        "cat",
        "dbt",
        after_changed_at="2026-08-01 12:00:00.000000",
        after_source_id=99,
        limit=10,
    )
    assert "source_changed_at > TIMESTAMP '2026-08-01 12:00:00.000000'" in sql
    assert "source_changed_at = TIMESTAMP '2026-08-01 12:00:00.000000' AND source_id > 99" in sql


def test_worklist_omits_the_predicate_with_no_cursor():
    sql = candidacy_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=10)
    assert "TIMESTAMP '" not in sql


@pytest.mark.parametrize(
    "after_changed_at,after_source_id",
    [("2026-08-01 12:00:00.000000", None), (None, 99)],
)
def test_worklist_treats_a_partial_cursor_as_no_cursor(after_changed_at, after_source_id):
    """Only one half of the pair supplied degrades to a full sweep, not an error."""
    sql = candidacy_worklist_sql(
        "cat",
        "dbt",
        after_changed_at=after_changed_at,
        after_source_id=after_source_id,
        limit=10,
    )
    assert "TIMESTAMP '" not in sql


def test_worklist_rejects_an_injected_schema():
    with pytest.raises(ValueError, match="dbt_schema"):
        candidacy_worklist_sql(
            "cat",
            "dbt; drop table x",
            after_changed_at=None,
            after_source_id=None,
            limit=10,
        )


def test_candidacy_worklist_unions_the_upcoming_ids_source():
    """The S3 feed omits many upcoming general-stage rosters the API race object carries."""
    sql = candidacy_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=10)
    assert "stg_airbyte_source__ballotready_s3_candidacies_v3" in sql
    assert "stg_airbyte_source__ballotready_api_race" in sql
    assert "stg_airbyte_source__ballotready_api_election" in sql


def test_candidacy_worklist_no_longer_reads_the_dbt_intermediate_layer():
    """The upcoming-ids source used to live in an int_ model; it is now inlined from staging only."""
    sql = candidacy_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=10)
    assert "int__ballotready_upcoming_candidacy_ids" not in sql


def test_candidacy_worklist_puts_both_sources_on_the_one_dbt_schema():
    sql = candidacy_worklist_sql("cat", "dbt_staging", after_changed_at=None, after_source_id=None, limit=10)
    assert "`dbt_staging`.`stg_airbyte_source__ballotready_s3_candidacies_v3`" in sql
    assert "`dbt_staging`.`stg_airbyte_source__ballotready_api_race`" in sql
    assert "`dbt_staging`.`stg_airbyte_source__ballotready_api_election`" in sql


def test_candidacy_worklist_accepts_and_ignores_source_schema():
    """A later builder (issues) needs source_schema; this one takes it for a uniform call site."""
    sql = candidacy_worklist_sql(
        "cat",
        "dbt",
        source_schema="zzz_marker_schema",
        after_changed_at=None,
        after_source_id=None,
        limit=10,
    )
    assert "zzz_marker_schema" not in sql


def test_geofence_worklist_reads_geofence_ids_off_the_candidacies_table():
    """Many candidacies share one geofence; the freshest of them decides its due time."""
    sql = geofence_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=10)
    assert "stg_airbyte_source__ballotready_s3_candidacies_v3" in sql
    assert "br_geofence_id" in sql
    assert "IS NOT NULL" in sql
    assert "GROUP BY source_id" in sql


def test_geofence_worklist_orders_by_the_keyset_pair_and_applies_the_limit():
    sql = geofence_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=25)
    assert "ORDER BY source_changed_at ASC, source_id ASC" in sql
    assert "LIMIT 25" in sql


def test_geofence_worklist_emits_a_keyset_predicate_when_a_cursor_is_present():
    sql = geofence_worklist_sql(
        "cat", "dbt", after_changed_at="2026-08-01 12:00:00.000000", after_source_id=5, limit=10
    )
    assert "source_changed_at > TIMESTAMP '2026-08-01 12:00:00.000000'" in sql
    assert "source_changed_at = TIMESTAMP '2026-08-01 12:00:00.000000' AND source_id > 5" in sql


def test_geofence_worklist_rejects_an_injected_catalog():
    with pytest.raises(ValueError, match="catalog"):
        geofence_worklist_sql(
            "cat; drop table x", "dbt", after_changed_at=None, after_source_id=None, limit=10
        )


def test_race_derived_worklist_explodes_filing_periods():
    """Filing period ids only exist nested in each race's filing_periods array."""
    sql = race_derived_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=10)
    assert "stg_airbyte_source__ballotready_api_race" in sql
    assert "LATERAL VIEW explode(filing_periods) AS filing_period" in sql
    assert "filing_period.databaseId" in sql
    assert "GROUP BY source_id" in sql


def test_race_derived_worklist_orders_by_the_keyset_pair_and_applies_the_limit():
    sql = race_derived_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=50)
    assert "ORDER BY source_changed_at ASC, source_id ASC" in sql
    assert "LIMIT 50" in sql


def test_race_derived_worklist_emits_a_keyset_predicate_when_a_cursor_is_present():
    sql = race_derived_worklist_sql(
        "cat", "dbt", after_changed_at="2026-08-01 12:00:00.000000", after_source_id=7, limit=10
    )
    assert "source_changed_at > TIMESTAMP '2026-08-01 12:00:00.000000'" in sql
    assert "source_changed_at = TIMESTAMP '2026-08-01 12:00:00.000000' AND source_id > 7" in sql


def test_race_derived_worklist_rejects_an_injected_dbt_schema():
    with pytest.raises(ValueError, match="dbt_schema"):
        race_derived_worklist_sql(
            "cat", "dbt`; drop table x", after_changed_at=None, after_source_id=None, limit=10
        )


def test_position_derived_worklist_reads_the_normalized_position_struct_without_exploding():
    sql = position_derived_worklist_sql(
        "cat", "dbt", field="normalized_position", after_changed_at=None, after_source_id=None, limit=10
    )
    assert "stg_airbyte_source__ballotready_api_position" in sql
    assert "normalized_position.databaseId" in sql
    assert "LATERAL VIEW" not in sql
    assert "GROUP BY source_id" in sql


def test_position_derived_worklist_explodes_election_frequencies():
    sql = position_derived_worklist_sql(
        "cat", "dbt", field="election_frequencies", after_changed_at=None, after_source_id=None, limit=10
    )
    assert "stg_airbyte_source__ballotready_api_position" in sql
    assert "LATERAL VIEW explode(election_frequencies) AS election_frequency" in sql
    assert "election_frequency.databaseId" in sql
    assert "GROUP BY source_id" in sql


def test_position_derived_worklist_rejects_an_unknown_field():
    with pytest.raises(ValueError, match="field"):
        position_derived_worklist_sql(
            "cat", "dbt", field="bogus", after_changed_at=None, after_source_id=None, limit=10
        )


def test_position_derived_worklist_orders_by_the_keyset_pair_and_applies_the_limit():
    sql = position_derived_worklist_sql(
        "cat", "dbt", field="normalized_position", after_changed_at=None, after_source_id=None, limit=15
    )
    assert "ORDER BY source_changed_at ASC, source_id ASC" in sql
    assert "LIMIT 15" in sql


def test_position_derived_worklist_emits_a_keyset_predicate_when_a_cursor_is_present():
    sql = position_derived_worklist_sql(
        "cat",
        "dbt",
        field="election_frequencies",
        after_changed_at="2026-08-01 12:00:00.000000",
        after_source_id=3,
        limit=10,
    )
    assert "source_changed_at > TIMESTAMP '2026-08-01 12:00:00.000000'" in sql
    assert "source_changed_at = TIMESTAMP '2026-08-01 12:00:00.000000' AND source_id > 3" in sql


def test_position_derived_worklist_rejects_an_injected_catalog():
    with pytest.raises(ValueError, match="catalog"):
        position_derived_worklist_sql(
            "cat; drop table x",
            "dbt",
            field="normalized_position",
            after_changed_at=None,
            after_source_id=None,
            limit=10,
        )


def test_issue_worklist_reads_issue_ids_out_of_landed_stance_payloads():
    sql = issue_worklist_sql("cat", "dbt", source_schema="src", limit=100)
    assert "ballotready_stance_raw" in sql
    assert "payload" in sql


def test_issue_worklist_excludes_ids_already_landed():
    sql = issue_worklist_sql("cat", "dbt", source_schema="src", limit=100)
    assert "ballotready_issue_raw" in sql
    assert "NOT EXISTS" in sql or "LEFT ANTI" in sql


def test_issue_worklist_correlates_the_anti_join_on_matching_ids():
    """Pin both the exact correlation predicate and which table it excludes against.

    A swapped correlation (e.g. comparing a column to itself) would make the
    anti-join a permanent no-op, or pointing it at the wrong table would make it
    exclude nothing real; either regression would still pass a substring check
    that only looks for "NOT EXISTS" and the table name somewhere in the SQL.
    """
    sql = issue_worklist_sql("cat", "dbt", source_schema="src", limit=100)
    issue_table = landing_table("cat", "src", "issue")
    correlation = (
        f"NOT EXISTS (SELECT 1 FROM {issue_table} landed WHERE landed.requested_id = referenced.source_id)"
    )
    assert correlation in sql
    assert "SELECT source_id, current_timestamp() AS source_changed_at FROM referenced" in sql


def test_issue_worklist_applies_the_limit():
    assert "LIMIT 100" in issue_worklist_sql("cat", "dbt", source_schema="src", limit=100)


def test_issue_worklist_rejects_an_injected_source_schema():
    with pytest.raises(ValueError, match="source_schema"):
        issue_worklist_sql("cat", "dbt", source_schema="src; drop table x", limit=10)


def test_issue_worklist_requires_source_schema():
    """Issue ids only exist in the landed stance table; there is no safe default schema."""
    with pytest.raises(ValueError, match="source_schema"):
        issue_worklist_sql("cat", "dbt", limit=10)


def test_issue_worklist_excludes_null_payloads():
    """A null payload records an id the API returned nothing for; it has no stances to read."""
    sql = issue_worklist_sql("cat", "dbt", source_schema="src", limit=10)
    assert "payload IS NOT NULL" in sql


def test_issue_worklist_accepts_the_full_uniform_kwarg_set():
    """A later task calls every worklist builder identically, without branching on entity name."""
    sql = issue_worklist_sql(
        "cat",
        "dbt",
        source_schema="src",
        after_changed_at="2026-08-01 12:00:00.000000",
        after_source_id=99,
        limit=10,
    )
    assert "ballotready_stance_raw" in sql
    assert "2026-08-01" not in sql


EXPECTED_ENTITIES = {
    "candidacy",
    "endorsement",
    "filing_period",
    "geofence",
    "issue",
    "normalized_position",
    "party",
    "position_election_frequency",
    "stance",
}


def test_registry_covers_exactly_the_nine_entities():
    assert set(ENTITY_SPECS) == EXPECTED_ENTITIES


# issue's worklist reads ids out of stance's landing table (see issue_worklist_sql); every
# other entity's worklist reads only dbt staging models. Spelled out per-entity, rather than
# just asserting issue's value, so a future entity added here without a reads_tables decision
# fails loudly instead of silently defaulting to ().
EXPECTED_READS_TABLES: dict[str, tuple[str, ...]] = {
    "candidacy": (),
    "party": (),
    "stance": (),
    "endorsement": (),
    "geofence": (),
    "filing_period": (),
    "normalized_position": (),
    "position_election_frequency": (),
    "issue": ("stance",),
}


def test_expected_reads_tables_covers_the_whole_registry():
    assert set(EXPECTED_READS_TABLES) == set(ENTITY_SPECS)


@pytest.mark.parametrize("entity,expected", sorted(EXPECTED_READS_TABLES.items()))
def test_registry_declares_reads_tables_explicitly(entity, expected):
    assert ENTITY_SPECS[entity].reads_tables == expected


def test_every_spec_ships_at_the_proven_page_size():
    """100 is the only size proven against the endpoint; raising it needs the probe."""
    assert {s.batch_size for s in ENTITY_SPECS.values()} == {100}


@pytest.mark.parametrize("entity", sorted(EXPECTED_ENTITIES))
def test_every_spec_has_a_selection_and_a_worklist_builder(entity):
    spec = ENTITY_SPECS[entity]
    assert spec.selection.strip()
    assert callable(spec.worklist_sql)


@pytest.mark.parametrize("entity", sorted(EXPECTED_ENTITIES))
def test_every_spec_key_matches_its_name(entity):
    assert ENTITY_SPECS[entity].name == entity


def test_candidacy_keyed_entities_share_the_candidacy_node_type():
    for entity in ("candidacy", "party", "stance", "endorsement"):
        assert ENTITY_SPECS[entity].node_type == "Candidacy"


def test_node_types_match_the_ballotready_object_names():
    assert ENTITY_SPECS["geofence"].node_type == "Geofence"
    assert ENTITY_SPECS["issue"].node_type == "Issue"
    assert ENTITY_SPECS["filing_period"].node_type == "FilingPeriod"
    assert ENTITY_SPECS["normalized_position"].node_type == "NormalizedPosition"
    assert ENTITY_SPECS["position_election_frequency"].node_type == "PositionElectionFrequency"


@pytest.mark.parametrize("entity", sorted(EXPECTED_ENTITIES))
def test_every_worklist_builder_accepts_the_uniform_signature(entity):
    """Every builder is called the same way, so the task body needs no branch.

    issue_worklist_sql raises ValueError when source_schema is missing, so it
    is passed here alongside the cursor kwargs and limit.
    """
    sql = ENTITY_SPECS[entity].worklist_sql(
        "cat",
        "dbt",
        source_schema="src",
        after_changed_at=None,
        after_source_id=None,
        limit=10,
    )
    assert isinstance(sql, str) and sql.strip()


def test_position_derived_specs_are_bound_to_distinct_fields():
    """A crossed binding would silently fetch the wrong id set for one of the two entities."""
    normalized_sql = ENTITY_SPECS["normalized_position"].worklist_sql("cat", "dbt", limit=10)
    frequency_sql = ENTITY_SPECS["position_election_frequency"].worklist_sql("cat", "dbt", limit=10)
    assert "normalized_position.databaseId" in normalized_sql
    assert "election_frequency.databaseId" not in normalized_sql
    assert "election_frequency.databaseId" in frequency_sql
    assert "normalized_position.databaseId" not in frequency_sql


CHANGED = {1: datetime(2026, 8, 1, 9, 0, 0), 2: datetime(2026, 8, 1, 10, 0, 0)}

_ROW_COLUMNS = (
    "requested_id",
    "node_id",
    "database_id",
    "payload",
    "source_changed_at",
    "extracted_at",
    "dag_run_id",
)


def _row_dict(row: tuple) -> dict:
    return dict(zip(_ROW_COLUMNS, row, strict=True))


def test_build_insert_rows_includes_one_row_per_requested_id_including_misses():
    fetched = [FetchedNode(1, {"databaseId": 11, "id": "abc"}), FetchedNode(2, None)]

    rows = build_insert_rows(fetched, CHANGED, "2026-08-25T00:00:00", "run-1")

    assert [row[0] for row in rows] == [1, 2]
    miss = _row_dict(rows[1])
    assert miss["payload"] is None
    assert miss["node_id"] is None
    assert miss["database_id"] is None


def test_build_insert_rows_stores_the_payload_as_a_json_string():
    fetched = [FetchedNode(1, {"databaseId": 11, "id": "abc"})]

    row = _row_dict(build_insert_rows(fetched, CHANGED, "2026-08-25T00:00:00", "run-1")[0])

    assert json.loads(row["payload"]) == {"databaseId": 11, "id": "abc"}
    assert row["database_id"] == 11
    assert row["node_id"] == "abc"


def test_build_insert_rows_carries_the_source_changed_at_that_put_the_id_on_the_worklist():
    row = _row_dict(build_insert_rows([FetchedNode(2, None)], CHANGED, "2026-08-25T00:00:00", "run-1")[0])
    assert row["source_changed_at"] == "2026-08-01 10:00:00.000000"


def test_build_insert_rows_stamps_the_run_and_extraction_time():
    row = _row_dict(build_insert_rows([FetchedNode(1, {})], CHANGED, "2026-08-25T00:00:00", "run-1")[0])
    assert row["dag_run_id"] == "run-1"
    assert row["extracted_at"] == "2026-08-25T00:00:00"


def test_build_insert_rows_of_no_fetched_is_empty():
    assert build_insert_rows([], CHANGED, "2026-08-25T00:00:00", "run-1") == []


def test_build_insert_rows_empty_dict_node_is_a_hit_not_a_miss():
    """An empty dict is a hit; node is None is the miss."""
    row = _row_dict(build_insert_rows([FetchedNode(1, {})], CHANGED, "2026-08-25T00:00:00", "run-1")[0])
    assert row["payload"] is not None
    assert row["node_id"] is None
    assert row["database_id"] is None


def test_create_landing_table_is_idempotent_and_fully_qualified():
    connection = FakeConnection([])
    create_landing_table(connection, "cat", "sch", "stance")
    sql = connection.cursor().executed[-1][0]
    assert "CREATE TABLE IF NOT EXISTS `cat`.`sch`.`ballotready_stance_raw`" in sql


def test_landing_table_declares_every_contract_column():
    connection = FakeConnection([])
    create_landing_table(connection, "cat", "sch", "stance")
    sql = connection.cursor().executed[-1][0]
    for column in (
        "requested_id",
        "node_id",
        "database_id",
        "payload",
        "source_changed_at",
        "extracted_at",
        "loaded_at",
        "dag_run_id",
    ):
        assert column in sql


_INSERT_ROW = (1, "abc", 11, '{"a": 1}', "2026-08-01 00:00:00.000000", "2026-08-25T00:00:00", "run-1")


def test_insert_rows_binds_parameters_and_casts_timestamps():
    connection = FakeConnection([])

    insert_rows(connection, "cat", "sch", "stance", [_INSERT_ROW])

    sql, parameters = connection.cursor().executed[-1]
    assert "INSERT INTO `cat`.`sch`.`ballotready_stance_raw`" in sql
    assert "cast(:source_changed_at_0 AS TIMESTAMP)" in sql
    assert "cast(:extracted_at_0 AS TIMESTAMP)" in sql
    assert "current_timestamp()" in sql
    assert parameters["requested_id_0"] == 1
    assert parameters["source_changed_at_0"] == "2026-08-01 00:00:00.000000"
    assert parameters["dag_run_id_0"] == "run-1"


def test_insert_rows_rejects_an_injected_schema():
    with pytest.raises(ValueError, match="schema"):
        insert_rows(FakeConnection([]), "cat", "sch; drop table x", "stance", [_INSERT_ROW])


def test_insert_rows_of_no_rows_executes_nothing():
    connection = FakeConnection([])
    insert_rows(connection, "cat", "sch", "stance", [])
    assert connection.cursor().executed == []


def test_insert_rows_chunks_at_insert_batch_size():
    """7 bound values/row * INSERT_BATCH_SIZE rows stays a deliberately conservative distance
    under the connector's (unverified from here) per-statement parameter cap.
    """
    connection = FakeConnection([])
    rows = [
        (i, None, None, None, "2026-08-01 00:00:00.000000", "2026-08-25T00:00:00", "run-1")
        for i in range(INSERT_BATCH_SIZE + 1)
    ]

    insert_rows(connection, "cat", "sch", "stance", rows)

    executed = connection.cursor().executed
    assert len(executed) == 2  # one full chunk, one with the single remaining row
    assert executed[0][0].count("(:requested_id_") == INSERT_BATCH_SIZE
    assert executed[1][0].count("(:requested_id_") == 1


def _ddl_column_names(sql: str) -> list[str]:
    """Column names declared in a `CREATE TABLE (...)` statement, in order."""
    inside = sql[sql.index("(") + 1 : sql.rindex(")")]
    return [part.strip().split()[0] for part in inside.split(",")]


def _insert_column_names(sql: str) -> list[str]:
    """Column names an `INSERT INTO t (...) VALUES ...` statement targets, in order."""
    start = sql.index("(", sql.index("INSERT INTO")) + 1
    end = sql.index(")", start)
    return [part.strip() for part in sql[start:end].split(",")]


def test_landing_table_declares_exactly_the_row_builder_columns_plus_loaded_at():
    """A renamed DDL column would otherwise land its row-builder counterpart as a silent NULL."""
    connection = FakeConnection([])
    create_landing_table(connection, "cat", "sch", "stance")
    ddl_columns = _ddl_column_names(connection.cursor().executed[-1][0])

    assert set(ddl_columns) == {*_ROW_COLUMNS, "loaded_at"}


def test_insert_rows_column_list_matches_the_landing_table_column_order():
    """The INSERT's column list must match the DDL's positions so a bound param lands right."""
    ddl_connection = FakeConnection([])
    create_landing_table(ddl_connection, "cat", "sch", "stance")
    ddl_columns = _ddl_column_names(ddl_connection.cursor().executed[-1][0])

    insert_connection = FakeConnection([])
    insert_rows(insert_connection, "cat", "sch", "stance", [_INSERT_ROW])
    insert_columns = _insert_column_names(insert_connection.cursor().executed[-1][0])

    assert insert_columns == ddl_columns


def _config(**overrides):
    base = dict(
        catalog="cat",
        dbt_schema="dbt",
        source_schema="src",
        api_token="tok",
        max_ids=1000,
        max_workers=2,
        requests_per_second=1000.0,
        full_reload=False,
        dag_run_id="run-1",
        extracted_at="2026-08-25T00:00:00",
    )
    base.update(overrides)
    return ExtractConfig(**base)


def test_extract_entity_returns_early_when_the_worklist_is_empty(monkeypatch):
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", lambda *a, **k: [])
    inserted = MagicMock()
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.insert_rows", inserted)

    summary = extract_entity(ENTITY_SPECS["issue"], FakeConnection([]), _config())

    assert summary["ids_requested"] == 0
    assert summary["rows_written"] == 0
    assert summary["windows"] == 0
    assert summary["cursor_source_changed_at"] is None
    inserted.assert_not_called()


def test_extract_entity_formats_the_cursor_timestamp_when_the_worklist_is_empty(monkeypatch):
    cursor_ts = datetime(2026, 8, 1, 9, 0, 0)
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.read_cursor", lambda *a, **k: (cursor_ts, 5)
    )
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", lambda *a, **k: [])

    summary = extract_entity(ENTITY_SPECS["issue"], FakeConnection([]), _config())

    assert summary["cursor_source_changed_at"] == "2026-08-01 09:00:00.000000"


def test_extract_entity_writes_one_insert_per_window_and_the_right_number_of_windows(monkeypatch):
    total_ids = WINDOW_SIZE * 2 + 50
    ids = [(i, datetime(2026, 8, 1)) for i in range(1, total_ids + 1)]
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", lambda *a, **k: ids)
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.fetch_nodes",
        lambda batch, *a, **k: [FetchedNode(i, {"databaseId": i, "id": "x"}) for i in batch],
    )
    inserted_calls = []
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.insert_rows",
        lambda conn, catalog, schema, entity, rows: inserted_calls.append(rows),
    )

    summary = extract_entity(ENTITY_SPECS["issue"], FakeConnection([]), _config())

    expected_windows = -(-total_ids // WINDOW_SIZE)  # ceil division
    assert len(inserted_calls) == expected_windows
    assert summary["windows"] == expected_windows
    assert summary["rows_written"] == total_ids
    assert sum(len(rows) for rows in inserted_calls) == total_ids
    assert len(inserted_calls[0]) == WINDOW_SIZE
    assert len(inserted_calls[-1]) == total_ids - WINDOW_SIZE * (expected_windows - 1)


def test_extract_entity_inserts_a_windows_rows_in_source_changed_at_requested_id_order(monkeypatch):
    """The correctness property the windowed insert depends on: rows within a window must land
    sorted by (source_changed_at, requested_id), not in whatever order threads happened to finish.
    """
    base = datetime(2026, 8, 1)
    # changed_at decreases as id increases, so a correct sort reverses id order -- this would pass
    # by accident if it merely preserved fetch/submission order instead of actually sorting.
    ids = [(i, base + timedelta(seconds=250 - i)) for i in range(1, 251)]
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", lambda *a, **k: ids)
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.fetch_nodes",
        lambda batch, *a, **k: [FetchedNode(i, {"databaseId": i, "id": "x"}) for i in batch],
    )
    inserted_calls = []
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.insert_rows",
        lambda conn, catalog, schema, entity, rows: inserted_calls.append(rows),
    )

    extract_entity(ENTITY_SPECS["issue"], FakeConnection([]), _config())

    assert len(inserted_calls) == 1
    assert [row[0] for row in inserted_calls[0]] == list(range(250, 0, -1))


def test_extract_entity_skips_the_cursor_on_full_reload(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.read_cursor",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("cursor must not be read")),
    )

    def _fake_read_worklist(conn, spec, config, after):
        # `after` is (None, None), a truthy tuple, so `... or []` would never
        # fall through; record it and return the empty worklist explicitly.
        seen["after"] = after
        return []

    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", _fake_read_worklist)

    extract_entity(ENTITY_SPECS["issue"], FakeConnection([]), _config(full_reload=True))

    assert seen["after"] == (None, None)


def test_extract_entity_aborts_before_the_failing_windows_insert(monkeypatch):
    """A worker failure must abort before that window's INSERT; earlier committed windows stay
    committed, or the cursor would skip the failed window's ids forever.
    """
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.WINDOW_SIZE", 100)
    ids = [(i, datetime(2026, 8, 1)) for i in range(1, 301)]  # three windows of 100
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", lambda *a, **k: ids)

    def flaky_fetch(batch, *a, **k):
        if batch[0] == 201:
            raise RuntimeError("boom")
        return [FetchedNode(i, {"databaseId": i, "id": "x"}) for i in batch]

    monkeypatch.setattr("include.custom_functions.ballotready_graphql.fetch_nodes", flaky_fetch)
    inserted_calls = []
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.insert_rows",
        lambda conn, catalog, schema, entity, rows: inserted_calls.append(rows),
    )

    with pytest.raises(RuntimeError, match="boom"):
        extract_entity(ENTITY_SPECS["issue"], FakeConnection([]), _config())

    assert len(inserted_calls) == 2  # the two windows before the failing one committed
    assert [row[0] for row in inserted_calls[0]] == list(range(1, 101))
    assert [row[0] for row in inserted_calls[1]] == list(range(101, 201))


_ALL_LANDING_TABLES = {name: landing_table("cat", "src", name) for name in ENTITY_SPECS}


class _SchemaAwareCursor:
    """Tracks which landing tables CREATE TABLE has touched and raises on a SELECT against
    any landing table that has not been created, the way Databricks raises
    TABLE_OR_VIEW_NOT_FOUND. Lets a test reproduce the original bug (a table read by one
    entity's worklist but never created because the entity that owns it did not run).
    """

    def __init__(self, known_tables: set[str]):
        self._known = known_tables
        self._result: list[tuple] = []

    def execute(self, sql, parameters=None):
        if sql.startswith("CREATE TABLE IF NOT EXISTS"):
            self._known.update(t for t in _ALL_LANDING_TABLES.values() if t in sql)
            self._result = []
            return
        if sql.startswith("CREATE SCHEMA"):
            self._result = []
            return
        referenced = {t for t in _ALL_LANDING_TABLES.values() if t in sql}
        missing = referenced - self._known
        if missing:
            raise RuntimeError(f"TABLE_OR_VIEW_NOT_FOUND: {sorted(missing)}")
        self._result = []

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result

    def close(self):
        pass


class _SchemaAwareConnection:
    def __init__(self, known_tables: set[str] | None = None):
        self.known_tables: set[str] = known_tables if known_tables is not None else set()

    def cursor(self, *args, **kwargs):
        return _SchemaAwareCursor(self.known_tables)


def test_extract_entity_for_issue_creates_its_own_table_and_the_stance_table(monkeypatch):
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", lambda *a, **k: [])
    connection = _SchemaAwareConnection()

    extract_entity(ENTITY_SPECS["issue"], connection, _config())

    assert connection.known_tables == {
        _ALL_LANDING_TABLES["issue"],
        _ALL_LANDING_TABLES["stance"],
    }


def test_extract_entity_with_no_reads_tables_creates_only_its_own_table(monkeypatch):
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.read_worklist", lambda *a, **k: [])
    connection = _SchemaAwareConnection()

    extract_entity(ENTITY_SPECS["candidacy"], connection, _config())

    assert connection.known_tables == {_ALL_LANDING_TABLES["candidacy"]}


def test_extract_entity_for_issue_against_an_existing_empty_stance_table_does_not_raise():
    """The regression this guards: with entities=["issue"], stance's task never runs. If
    extract_entity did not also create stance's table, issue_worklist_sql's anti-join against
    it would raise TABLE_OR_VIEW_NOT_FOUND instead of reaching the empty-worklist return.
    """
    connection = _SchemaAwareConnection(known_tables={_ALL_LANDING_TABLES["stance"]})

    summary = extract_entity(ENTITY_SPECS["issue"], connection, _config())

    assert summary["ids_requested"] == 0
    assert summary["rows_written"] == 0
    assert summary["windows"] == 0


class _LandingCursor:
    """A cursor whose execute() understands the three statements extract_entity issues -- DDL
    (no-op), the max-cursor SELECT, and a multi-row INSERT -- against a shared in-memory table.
    Lets read_cursor and insert_rows run for real, rather than mocking the property under test.
    """

    def __init__(self, rows: list[tuple[int, datetime]]):
        self._rows = rows
        self._result: list[tuple] = []

    def execute(self, sql, parameters=None):
        if sql.startswith(("CREATE SCHEMA", "CREATE TABLE")):
            self._result = []
        elif sql.startswith("SELECT source_changed_at, requested_id FROM"):
            if self._rows:
                requested_id, changed_at = max(self._rows, key=lambda r: (r[1], r[0]))
                self._result = [(changed_at, requested_id)]
            else:
                self._result = []
        elif sql.startswith("INSERT INTO"):
            parameters = parameters or {}
            i = 0
            while f"requested_id_{i}" in parameters:
                self._rows.append(
                    (
                        parameters[f"requested_id_{i}"],
                        datetime.fromisoformat(parameters[f"source_changed_at_{i}"]),
                    )
                )
                i += 1
            self._result = []
        else:
            raise AssertionError(f"unexpected SQL in this fake: {sql}")

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result

    def close(self):
        pass


class _LandingConnection:
    """An in-memory landing table, shared across two extract_entity calls (a run, then its retry)."""

    def __init__(self):
        self.rows: list[tuple[int, datetime]] = []

    def cursor(self, *args, **kwargs):
        return _LandingCursor(self.rows)


def _fake_read_worklist_over(full_worklist):
    """A worklist builder that applies the real keyset filter in Python, so the retry in the test
    below reads a worklist that actually reflects the cursor read_cursor already reported.
    """

    def _read(conn, spec, config, after):
        after_changed_at, after_source_id = after
        if after_changed_at is None:
            return list(full_worklist)
        return [
            (source_id, changed_at)
            for source_id, changed_at in full_worklist
            if (changed_at, source_id) > (after_changed_at, after_source_id)
        ]

    return _read


def test_committed_windows_form_a_cursor_prefix_so_a_retry_resumes_after_them(monkeypatch):
    """A crash mid-run must not skip or duplicate ids: earlier windows are already committed, and
    the retry's cursor read must resume exactly where the crash left off.
    """
    monkeypatch.setattr("include.custom_functions.ballotready_graphql.WINDOW_SIZE", 2)
    base = datetime(2026, 8, 1)
    full_worklist = [(i, base + timedelta(seconds=i)) for i in range(1, 6)]  # 5 ids, ascending
    monkeypatch.setattr(
        "include.custom_functions.ballotready_graphql.read_worklist",
        _fake_read_worklist_over(full_worklist),
    )

    attempted = []

    def flaky_fetch(batch, *a, **k):
        attempted.extend(batch)
        if batch == [3, 4] and attempted.count(3) == 1:
            raise RuntimeError("boom")
        return [FetchedNode(i, {"databaseId": i, "id": "x"}) for i in batch]

    monkeypatch.setattr("include.custom_functions.ballotready_graphql.fetch_nodes", flaky_fetch)

    connection = _LandingConnection()
    config = _config()

    with pytest.raises(RuntimeError, match="boom"):
        extract_entity(ENTITY_SPECS["issue"], connection, config)

    # Window 1 ([1, 2]) landed; window 2 ([3, 4]) never did.
    assert sorted(row[0] for row in connection.rows) == [1, 2]

    extract_entity(ENTITY_SPECS["issue"], connection, config)

    landed_ids = [row[0] for row in connection.rows]
    assert sorted(landed_ids) == [1, 2, 3, 4, 5]
    assert len(landed_ids) == len(set(landed_ids))  # no duplicates: the retry did not redo window 1


def test_make_session_pool_covers_every_worker():
    """Below max_workers in the pool, extra threads queue on the pool and gain nothing."""
    session = make_session(8)
    adapter = session.get_adapter("https://bpi.civicengine.com/graphql")
    assert adapter._pool_connections >= 8
    assert adapter._pool_maxsize >= 8
