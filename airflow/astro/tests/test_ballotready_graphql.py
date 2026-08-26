import base64
from dataclasses import FrozenInstanceError
from datetime import datetime

import pytest
from include.custom_functions.ballotready_graphql import (
    CANDIDACY_SELECTION,
    ENDORSEMENT_SELECTION,
    FILING_PERIOD_SELECTION,
    GEOFENCE_SELECTION,
    ISSUE_SELECTION,
    NORMALIZED_POSITION_SELECTION,
    PARTY_SELECTION,
    POSITION_ELECTION_FREQUENCY_SELECTION,
    STANCE_SELECTION,
    EntitySpec,
    FetchedNode,
    RateLimiter,
    candidacy_worklist_sql,
    chunked,
    encode_node_id,
    fetch_nodes,
    geofence_worklist_sql,
    is_retryable_status,
    issue_worklist_sql,
    landing_table,
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
    """Returns queued responses in order and records the ids each call requested."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requested_id_counts = []

    def post(self, url, json, headers, timeout):
        self.requested_id_counts.append(len(json["variables"]["ids"]))
        return self._responses.pop(0)


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
    sql = candidacy_worklist_sql(
        "cat", "dbt", intermediate_schema="dbtint", after_changed_at=None, after_source_id=None, limit=500
    )
    assert "ORDER BY source_changed_at ASC, source_id ASC" in sql
    assert "LIMIT 500" in sql


def test_worklist_emits_a_keyset_predicate_when_a_cursor_is_present():
    sql = candidacy_worklist_sql(
        "cat",
        "dbt",
        intermediate_schema="dbtint",
        after_changed_at="2026-08-01 12:00:00.000000",
        after_source_id=99,
        limit=10,
    )
    assert "source_changed_at > TIMESTAMP '2026-08-01 12:00:00.000000'" in sql
    assert "source_changed_at = TIMESTAMP '2026-08-01 12:00:00.000000' AND source_id > 99" in sql


def test_worklist_omits_the_predicate_with_no_cursor():
    sql = candidacy_worklist_sql(
        "cat", "dbt", intermediate_schema="dbtint", after_changed_at=None, after_source_id=None, limit=10
    )
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
        intermediate_schema="dbtint",
        after_changed_at=after_changed_at,
        after_source_id=after_source_id,
        limit=10,
    )
    assert "TIMESTAMP '" not in sql


def test_worklist_rejects_an_injected_schema():
    with pytest.raises(ValueError, match="staging_schema"):
        candidacy_worklist_sql(
            "cat",
            "dbt; drop table x",
            intermediate_schema="dbtint",
            after_changed_at=None,
            after_source_id=None,
            limit=10,
        )


def test_candidacy_worklist_unions_the_upcoming_ids_source():
    """The S3 feed omits many upcoming general-stage rosters the API race object carries."""
    sql = candidacy_worklist_sql(
        "cat", "dbt", intermediate_schema="dbtint", after_changed_at=None, after_source_id=None, limit=10
    )
    assert "stg_airbyte_source__ballotready_s3_candidacies_v3" in sql
    assert "int__ballotready_upcoming_candidacy_ids" in sql


def test_candidacy_worklist_puts_each_source_on_its_own_schema():
    """The two sources live in different schemas in the real catalog; one param cannot address both."""
    sql = candidacy_worklist_sql(
        "cat",
        "dbt_staging",
        intermediate_schema="dbt_intermediate",
        after_changed_at=None,
        after_source_id=None,
        limit=10,
    )
    assert "`dbt_staging`.`stg_airbyte_source__ballotready_s3_candidacies_v3`" in sql
    assert "`dbt_intermediate`.`int__ballotready_upcoming_candidacy_ids`" in sql


def test_candidacy_worklist_requires_intermediate_schema():
    """The upcoming-ids source cannot be addressed without it; there is no safe default."""
    with pytest.raises(ValueError, match="intermediate_schema"):
        candidacy_worklist_sql("cat", "dbt", after_changed_at=None, after_source_id=None, limit=10)


def test_candidacy_worklist_rejects_an_injected_intermediate_schema():
    with pytest.raises(ValueError, match="intermediate_schema"):
        candidacy_worklist_sql(
            "cat",
            "dbt",
            intermediate_schema="dbt; drop table x",
            after_changed_at=None,
            after_source_id=None,
            limit=10,
        )


def test_candidacy_worklist_accepts_and_ignores_source_schema():
    """A later builder (issues) needs source_schema; this one takes it for a uniform call site."""
    sql = candidacy_worklist_sql(
        "cat",
        "dbt",
        intermediate_schema="dbtint",
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


def test_race_derived_worklist_rejects_an_injected_staging_schema():
    with pytest.raises(ValueError, match="staging_schema"):
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
        intermediate_schema="dbtint",
        source_schema="src",
        after_changed_at="2026-08-01 12:00:00.000000",
        after_source_id=99,
        limit=10,
    )
    assert "ballotready_stance_raw" in sql
    assert "dbtint" not in sql
    assert "2026-08-01" not in sql
