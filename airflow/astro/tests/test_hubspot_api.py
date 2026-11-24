"""Contracts for the HubSpot companion loader's activity-to-contact associations.

Every response here is a recorded fixture transcribed from the documented v4 batch-read
shape, never a live call: the loader's token is read at runtime from an Airflow Variable
and no test or worker holds it. What these tests pin is the part a changing endpoint would
break silently, which is the three decisions the landing grain depends on:

- one batch request per 1,000 ids, never above the endpoint's ceiling;
- one snapshot row per activity, with an explicit empty array when it has no links, so a
  removal of the last link is representable;
- a worklist that selects only activities at or past the cursor, plus ones that never
  landed.
"""

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from include.custom_functions.hubspot_api import (
    ACTIVITY_OBJECTS,
    ASSOCIATION_BATCH_SIZE,
    INSERT_BATCH_SIZE,
    INSERT_COLUMNS,
    LANDING_COLUMNS,
    NO_ASSOCIATIONS_SUBCATEGORIES,
    AssociationExtractionConfig,
    LandedAssociation,
    _post,
    _redact,
    activity_worklist_sql,
    build_snapshot_rows,
    chunk_rows_for_insert,
    create_landing_table,
    extract_activity_associations,
    fetch_contact_associations,
    format_cursor_ts,
    insert_rows,
    is_retryable_status,
    landing_relation,
    parse_association_batch,
    read_cursor,
    retry_wait_seconds,
    validate_identifier,
    validate_source_prefix,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "hubspot_api"


def _fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None, text=""):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.headers = headers or {}
        self.text = text or json.dumps(self._body)

    def json(self):
        return self._body


class FakeSession:
    """Returns queued responses in order and records every request body it was sent."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []
        self.urls = []

    def post(self, url, json, headers, timeout):
        self.requests.append(json)
        self.urls.append(url)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeCursor:
    def __init__(self, rows=None, cursor_row=None):
        self.rows = list(rows or [])
        self.cursor_row = cursor_row
        self.executed = []
        self._pending = []

    def execute(self, sql, parameters=None):
        self.executed.append((sql, parameters))
        # A worklist query's rows are served on fetchmany; anything else gets none.
        self._pending = self.rows if sql.lstrip().upper().startswith(("SELECT", "WITH")) else []

    def fetchmany(self, size):
        chunk, self._pending = self._pending[:size], self._pending[size:]
        return chunk

    def fetchone(self):
        return self.cursor_row

    def close(self):
        pass


class FakeConnection:
    """Serves `rows` to the worklist query and `cursor_row` to a cursor read.

    They are separate because the two queries have different shapes: the worklist returns
    (activity_id, source_changed_at), and the cursor read returns (source_changed_at,
    activity_id).
    """

    def __init__(self, rows=None, cursor_row=None):
        self._cursor = FakeCursor(rows, cursor_row)

    def cursor(self, *args, **kwargs):
        return self._cursor


# --- the API contract -------------------------------------------------------


def test_a_linked_activity_yields_its_contact_ids():
    body = _fixture("associations_linked.json")

    parsed = parse_association_batch(body, ["101"])

    assert parsed == {"101": ["9001", "9002"]}


def test_an_activity_absent_from_the_response_yields_an_explicit_empty_array():
    """HubSpot omits an object with no associations. The id must still be present, empty."""
    body = _fixture("associations_linked.json")

    parsed = parse_association_batch(body, ["101", "102"])

    assert parsed["102"] == []
    assert set(parsed) == {"101", "102"}


def test_an_empty_to_list_also_yields_an_explicit_empty_array():
    body = {"results": [{"from": {"id": "101"}, "to": []}]}

    assert parse_association_batch(body, ["101"]) == {"101": []}


def test_every_to_object_id_is_taken_whatever_its_association_type():
    """Filtering on typeId would drop a link carrying only the unlabeled type."""
    body = _fixture("associations_all_types.json")

    parsed = parse_association_batch(body, ["101"])

    assert parsed == {"101": ["9001", "9002"]}


def test_a_repeated_contact_id_lands_once():
    body = {
        "results": [
            {
                "from": {"id": "101"},
                "to": [
                    {"toObjectId": 9001, "associationTypes": [{"typeId": 194}]},
                    {"toObjectId": 9001, "associationTypes": [{"typeId": 195}]},
                ],
            }
        ]
    }

    assert parse_association_batch(body, ["101"]) == {"101": ["9001"]}


def test_contact_ids_are_sorted_so_the_snapshot_is_stable():
    body = {
        "results": [
            {
                "from": {"id": "101"},
                "to": [{"toObjectId": 9003}, {"toObjectId": 9001}, {"toObjectId": 9002}],
            }
        ]
    }

    assert parse_association_batch(body, ["101"]) == {"101": ["9001", "9002", "9003"]}


def test_results_are_keyed_by_from_id_not_by_position():
    """The batch endpoint does not promise to answer in request order."""
    body = {
        "results": [
            {"from": {"id": "102"}, "to": [{"toObjectId": 9002}]},
            {"from": {"id": "101"}, "to": [{"toObjectId": 9001}]},
        ]
    }

    assert parse_association_batch(body, ["101", "102"]) == {"101": ["9001"], "102": ["9002"]}


def test_a_result_for_an_unrequested_activity_raises():
    body = {"results": [{"from": {"id": "999"}, "to": [{"toObjectId": 9001}]}]}

    with pytest.raises(RuntimeError, match="did not request"):
        parse_association_batch(body, ["101"])


def test_a_batch_that_reports_errors_raises_rather_than_landing_no_links():
    """An errored id landed as 'no links' is indistinguishable from a real removal."""
    body = {"results": [{"from": {"id": "101"}, "to": []}], "errors": [{"message": "boom"}]}

    with pytest.raises(RuntimeError, match="reported errors"):
        parse_association_batch(body, ["101"])


def test_a_207_no_associations_error_lands_an_explicit_empty_array():
    """HubSpot answers 207 with results and errors together when some inputs have no links.

    A quarter or more of tasks have none, so raising on this shape would fail every run and
    the empty-array path this loader exists for would never execute.
    """
    body = _fixture("associations_partial_207.json")

    parsed = parse_association_batch(body, ["201", "202"])

    assert parsed["201"] == ["9001"]
    assert parsed["202"] == []


def test_a_real_error_beside_a_no_associations_error_still_raises():
    body = _fixture("associations_partial_207.json")
    body["errors"].append(
        {"id": "203", "subCategory": "crm.associations.INVALID_OBJECT_TYPE", "message": "boom"}
    )

    with pytest.raises(RuntimeError, match="reported errors"):
        parse_association_batch(body, ["201", "202", "203"])


def test_a_no_associations_error_nested_on_the_entry_is_tolerated():
    """The subCategory can sit on a nested error rather than the outer entry."""
    body = {
        "results": [{"from": {"id": "101"}, "to": []}],
        "errors": [
            {
                "id": "101",
                "errors": [{"subCategory": "crm.associations.NO_ASSOCIATIONS_FOUND"}],
            }
        ],
    }

    assert parse_association_batch(body, ["101"]) == {"101": []}


def test_the_tolerated_subcategories_do_not_include_a_generic_validation_error():
    """Only 'there are none' is tolerated: a generic VALIDATION_ERROR is a real failure."""
    assert "VALIDATION_ERROR" not in NO_ASSOCIATIONS_SUBCATEGORIES
    assert all(value.startswith("crm.associations.") for value in NO_ASSOCIATIONS_SUBCATEGORIES)


def test_a_paged_result_is_re_requested_until_it_stops_paging():
    """A truncated association list would land as a partial snapshot, not as an error."""
    first = _fixture("associations_paged.json")
    second = {
        "results": [
            {"from": {"id": "301"}, "to": [{"toObjectId": 9002}]},
        ]
    }
    session = FakeSession([FakeResponse(body=first), FakeResponse(body=second)])

    associations = fetch_contact_associations(session, "calls", ["301"], "tok")

    assert associations == {"301": ["9001", "9002"]}
    assert len(session.requests) == 2
    # The continuation token travels with the same id, as the docs specify.
    assert session.requests[1] == {"inputs": [{"id": "301", "after": "302-cursor"}]}


def test_a_continuation_token_that_never_stops_raises_rather_than_looping():
    endless = _fixture("associations_paged.json")
    session = FakeSession([FakeResponse(body=endless) for _ in range(3)])

    with pytest.raises(RuntimeError, match="continuation token"):
        fetch_contact_associations(session, "calls", ["301"], "tok", max_pages=3)


# --- batching at the 1,000 boundary ----------------------------------------


@pytest.mark.parametrize(
    ("count", "expected_batches"),
    [
        (0, 0),
        (1, 1),
        (999, 1),
        (1000, 1),
        (1001, 2),
        (2000, 2),
        (2001, 3),
    ],
)
def test_batch_grouping_at_the_boundary(count, expected_batches):
    ids = [str(1000 + i) for i in range(count)]
    session = FakeSession([FakeResponse(body={"results": []}) for _ in range(expected_batches)])

    fetch_contact_associations(session, "calls", ids, "tok")

    assert len(session.requests) == expected_batches
    assert sum(len(body["inputs"]) for body in session.requests) == count
    assert all(len(body["inputs"]) <= ASSOCIATION_BATCH_SIZE for body in session.requests)
    # Every id is requested exactly once, in order.
    requested = [item["id"] for body in session.requests for item in body["inputs"]]
    assert requested == ids


def test_a_batch_over_the_endpoint_ceiling_is_refused():
    session = FakeSession([])

    with pytest.raises(ValueError, match="batch_size"):
        fetch_contact_associations(session, "calls", ["1"], "tok", batch_size=ASSOCIATION_BATCH_SIZE + 1)
    assert session.requests == []


def test_the_endpoint_is_the_v4_associations_batch_read_for_the_activity_object():
    session = FakeSession([FakeResponse(body={"results": []})])

    fetch_contact_associations(session, "meetings", ["7"], "tok")

    assert session.urls == ["https://api.hubapi.com/crm/v4/associations/meetings/contacts/batch/read"]
    assert session.requests == [{"inputs": [{"id": "7"}]}]


def test_no_activity_ids_issues_no_request():
    session = FakeSession([])

    assert fetch_contact_associations(session, "calls", [], "tok") == {}
    assert session.requests == []


def test_every_activity_object_uses_a_valid_identifier():
    for activity_object, _ in ACTIVITY_OBJECTS:
        assert validate_identifier("activity object", activity_object) == activity_object


def test_the_four_activity_types_are_the_scope():
    """Email is absent on purpose: Lakeflow's single contact id already covers 99.6%."""
    assert [activity_object for activity_object, _ in ACTIVITY_OBJECTS] == [
        "calls",
        "meetings",
        "notes",
        "tasks",
    ]


# --- retries ---------------------------------------------------------------


@pytest.mark.parametrize("status", [429, 500, 502, 503])
def test_retryable_statuses(status):
    assert is_retryable_status(status)


@pytest.mark.parametrize("status", [200, 207, 400, 401, 403, 404, 422])
def test_non_retryable_statuses(status):
    assert not is_retryable_status(status)


def test_a_429_is_retried_and_the_retry_after_header_is_honoured():
    session = FakeSession(
        [
            FakeResponse(status_code=429, headers={"Retry-After": "3"}),
            FakeResponse(body={"results": []}),
        ]
    )
    slept = []

    body = _post(
        session,
        "/crm/v4/associations/calls/contacts/batch/read",
        {"inputs": []},
        "tok",
        "1 id",
        sleep=slept.append,
    )

    assert body == {"results": []}
    assert slept == [3.0]
    assert len(session.requests) == 2


def test_a_403_is_not_retried_and_names_the_status():
    session = FakeSession([FakeResponse(status_code=403, text="MISSING_SCOPES")])
    with pytest.raises(RuntimeError, match="403"):
        _post(session, "/x", {}, "tok", "1 id", sleep=lambda _: None)
    assert len(session.requests) == 1


def test_an_empty_token_is_refused_before_any_request():
    session = FakeSession([])

    with pytest.raises(ValueError, match="hubspot_companion_loader_token"):
        _post(session, "/x", {}, "   ", "1 id")
    assert session.requests == []


def test_the_token_never_appears_in_a_raised_message():
    session = FakeSession([FakeResponse(status_code=400, text="bad header: Bearer pat-secret-value")])

    with pytest.raises(RuntimeError) as excinfo:
        _post(session, "/x", {}, "pat-secret-value", "1 id", sleep=lambda _: None)

    assert "pat-secret-value" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


def test_redact_leaves_a_message_alone_without_a_secret():
    assert _redact("nothing to hide", "") == "nothing to hide"
    assert _redact("a tok b", "tok") == "a *** b"


def test_retry_wait_honours_and_caps_retry_after():
    assert retry_wait_seconds({"Retry-After": "12"}, attempt=0) == 12.0
    assert retry_wait_seconds({"retry-after": "5"}, attempt=0) == 5.0
    assert retry_wait_seconds({"Retry-After": "9999"}, attempt=0, max_backoff=60.0) == 60.0
    assert retry_wait_seconds({"Retry-After": "soon"}, attempt=2, base_backoff=1.0) == 4.0


# --- the landing grain -----------------------------------------------------


def test_build_snapshot_rows_emits_one_row_per_activity_including_empty_ones():
    associations = {"101": ["9001", "9002"], "102": []}
    changed_at = {"101": datetime(2026, 11, 1), "102": datetime(2026, 11, 2)}

    rows = build_snapshot_rows(associations, changed_at, "CALL", "2026-11-24T00:00:00", "run-1")

    assert len(rows) == 2
    by_id = {row.activity_id: row for row in rows}
    assert json.loads(by_id["101"].contact_ids_json) == ["9001", "9002"]
    # The empty array is a row, and its value is `[]`, not null and not a missing row.
    assert by_id["102"].contact_ids_json == "[]"
    assert by_id["102"].activity_type == "CALL"


def test_snapshot_rows_are_ordered_by_the_cursor_pair():
    associations = {"103": [], "101": [], "102": []}
    changed_at = {
        "103": datetime(2026, 11, 1),
        "101": datetime(2026, 11, 2),
        "102": datetime(2026, 11, 1),
    }

    rows = build_snapshot_rows(associations, changed_at, "NOTE", "2026-11-24T00:00:00", "run-1")

    assert [(row.source_changed_at, row.activity_id) for row in rows] == [
        ("2026-11-01 00:00:00.000000", "102"),
        ("2026-11-01 00:00:00.000000", "103"),
        ("2026-11-02 00:00:00.000000", "101"),
    ]


def test_a_missing_changed_at_raises_rather_than_landing_a_null_cursor():
    """A null source_changed_at would be skipped by the cursor's max() without an error."""
    with pytest.raises(KeyError):
        build_snapshot_rows({"101": []}, {}, "CALL", "2026-11-24T00:00:00", "run-1")


def test_build_snapshot_rows_rejects_an_activity_type_that_is_not_a_bare_label():
    with pytest.raises(ValueError, match="activity type"):
        build_snapshot_rows({}, {}, "CALL' or 1=1 --", "2026-11-24T00:00:00", "run-1")


def test_the_landing_contract_columns_and_the_row_fields_agree():
    assert LandedAssociation._fields == INSERT_COLUMNS
    assert [name for name, _ in LANDING_COLUMNS] == [
        "activity_type",
        "activity_id",
        "contact_ids_json",
        "source_changed_at",
        "extracted_at",
        "loaded_at",
        "dag_run_id",
    ]


def test_create_landing_table_names_the_companion_table_and_every_column():
    connection = FakeConnection()

    create_landing_table(connection, "raw", "hubspot")

    sql = connection.cursor().executed[-1][0]
    assert "CREATE TABLE IF NOT EXISTS `raw`.`hubspot`.`companion_activity_contact_associations`" in sql
    for column, _ in LANDING_COLUMNS:
        assert column in sql
    # The schema is the owner's to create; a job that created it would own it.
    assert "CREATE SCHEMA" not in sql


def test_insert_rows_is_an_insert_and_binds_the_timestamp_columns():
    connection = FakeConnection()
    row = LandedAssociation(
        activity_type="CALL",
        activity_id="101",
        contact_ids_json="[]",
        source_changed_at="2026-11-01 00:00:00.000000",
        extracted_at="2026-11-24T00:00:00",
        dag_run_id="run-1",
    )

    insert_rows(connection, "dev", "hubspot", [row])

    sql, parameters = connection.cursor().executed[-1]
    assert sql.startswith("INSERT INTO `dev`.`hubspot`.`companion_activity_contact_associations`")
    assert "cast(:source_changed_at_0 AS TIMESTAMP)" in sql
    assert "current_timestamp()" in sql
    assert "MERGE" not in sql
    assert parameters["contact_ids_json_0"] == "[]"


def test_insert_rows_of_no_rows_executes_nothing():
    connection = FakeConnection()
    insert_rows(connection, "dev", "hubspot", [])
    assert connection.cursor().executed == []


def test_insert_rows_chunks_at_the_row_cap():
    connection = FakeConnection()
    rows = [
        LandedAssociation("CALL", str(i), "[]", "2026-11-01 00:00:00.000000", "2026-11-24T00:00:00", "run-1")
        for i in range(INSERT_BATCH_SIZE + 1)
    ]

    insert_rows(connection, "dev", "hubspot", rows)

    statements = [sql for sql, _ in connection.cursor().executed if sql.startswith("INSERT")]
    assert len(statements) == 2


def test_chunk_rows_bounds_on_parameter_characters_not_only_row_count():
    wide = "9" * 400
    rows = [
        LandedAssociation("CALL", str(i), json.dumps([wide] * 5), "2026-11-01", "2026-11-24", "run-1")
        for i in range(10)
    ]

    chunks = chunk_rows_for_insert(rows, max_chars=3000, max_rows=INSERT_BATCH_SIZE)

    assert len(chunks) > 1
    assert sum(len(chunk) for chunk in chunks) == 10


# --- the worklist ----------------------------------------------------------


def _worklist(**kwargs):
    return activity_worklist_sql(
        source_prefix="goodparty_data_catalog.lakeflow_source_dev",
        activity_object="calls",
        activity_type="CALL",
        landing_table="`dev`.`hubspot`.`companion_activity_contact_associations`",
        **kwargs,
    )


def test_the_worklist_reads_the_configured_source_relation():
    sql = _worklist()

    assert "`goodparty_data_catalog`.`lakeflow_source_dev`.`calls`" in sql


def test_the_worklist_relation_follows_the_config_value():
    """This is what lets the D3 cutover move the tables without a code change."""
    sql = activity_worklist_sql(
        source_prefix="raw.hubspot",
        activity_object="calls",
        activity_type="CALL",
        landing_table="`raw`.`hubspot`.`companion_activity_contact_associations`",
    )

    assert "`raw`.`hubspot`.`calls`" in sql
    assert "lakeflow_source_dev" not in sql


@pytest.mark.parametrize("prefix", ["", "raw", "raw.hubspot.extra", "raw hubspot", "raw.hub;spot"])
def test_the_worklist_prefix_must_be_a_bare_catalog_and_schema(prefix):
    with pytest.raises(ValueError):
        validate_source_prefix(prefix)


def test_the_worklist_selects_only_activities_at_or_past_the_cursor():
    sql = _worklist(after_changed_at=datetime(2026, 11, 1, 12, 0), after_activity_id="101")

    assert "hs_lastmodifieddate IS NOT NULL" in sql
    assert "hs_lastmodifieddate >= TIMESTAMP '2026-11-01 12:00:00.000000'" in sql
    assert "source_changed_at > TIMESTAMP '2026-11-01 12:00:00.000000'" in sql
    assert "source_changed_at = TIMESTAMP '2026-11-01 12:00:00.000000' and activity_id > '101'" in sql


def test_the_worklist_pushes_the_cursor_floor_into_the_keyset_scan():
    """Without the floor the aggregate scans the whole table on every incremental run.

    It has to sit inside the scan fragment that renames the column, so it is asserted in
    the scan rather than after the alias.
    """
    sql = _worklist(after_changed_at=datetime(2026, 11, 1, 12, 0), after_activity_id="101")

    floor = sql.index("hs_lastmodifieddate >= TIMESTAMP '2026-11-01 12:00:00.000000'")
    assert floor < sql.index("GROUP BY activity_id")
    # Naming the pre-rename column outside its own scan would not bind at all.
    assert "scanned WHERE activity_id IS NOT NULL AND hs_lastmodifieddate" not in sql


def test_the_unseen_branch_is_not_floored_at_the_cursor():
    """The floor is exactly what would hide a late arrival behind the cursor."""
    sql = _worklist(after_changed_at=datetime(2026, 11, 1, 12, 0), after_activity_id="101")

    unseen = sql[sql.index("unseen AS (") :]
    assert "hs_lastmodifieddate >= TIMESTAMP" not in unseen


def test_the_worklist_adds_never_landed_activities_behind_the_cursor():
    """A late backfill behind the cursor is only reachable through the unseen branch."""
    sql = _worklist(after_changed_at=datetime(2026, 11, 1, 12, 0), after_activity_id="101")

    assert "NOT EXISTS" in sql
    assert "landed.activity_type = 'CALL'" in sql
    assert "landed.activity_id = k.activity_id" in sql
    assert "UNION ALL" in sql


def test_a_full_sweep_collapses_to_one_row_per_activity_and_omits_the_unseen_branch():
    sql = _worklist()

    assert "GROUP BY activity_id" in sql
    assert "NOT EXISTS" not in sql
    assert "UNION ALL" not in sql
    # No cursor, so nothing is floored and nothing needs the keyset pair.
    assert "hs_lastmodifieddate >= TIMESTAMP" not in sql
    assert "activity_id > '" not in sql


def test_the_worklist_is_ordered_by_the_cursor_pair():
    assert _worklist().rstrip().endswith("ORDER BY source_changed_at ASC, activity_id ASC")


@pytest.mark.parametrize("activity_type", ["", "call", "CALL' --", "1CALL"])
def test_the_worklist_rejects_an_activity_type_that_is_not_a_bare_label(activity_type):
    with pytest.raises(ValueError, match="activity type"):
        activity_worklist_sql(
            source_prefix="raw.hubspot",
            activity_object="calls",
            activity_type=activity_type,
            landing_table="`raw`.`hubspot`.`t`",
        )


def test_a_partial_cursor_reads_as_no_cursor():
    """An over-broad worklist is safe; raising here would stall a run on a partial value."""
    assert "NOT EXISTS" not in _worklist(after_changed_at=datetime(2026, 11, 1))
    assert "NOT EXISTS" not in _worklist(after_activity_id="101")


def test_an_injected_cursor_id_is_refused():
    with pytest.raises(ValueError, match="activity id"):
        _worklist(after_changed_at=datetime(2026, 11, 1), after_activity_id="1' or '1'='1")


# --- the cursor ------------------------------------------------------------


def test_read_cursor_returns_none_when_nothing_landed():
    assert read_cursor(FakeConnection(cursor_row=None), "dev", "hubspot", "CALL") == (None, None)


def test_read_cursor_returns_the_highest_landed_pair_for_the_activity_type():
    landed = datetime(2026, 11, 1, 12, 0)

    assert read_cursor(FakeConnection(cursor_row=(landed, "101")), "dev", "hubspot", "CALL") == (
        landed,
        "101",
    )


def test_read_cursor_filters_by_activity_type():
    connection = FakeConnection([])
    read_cursor(connection, "dev", "hubspot", "CALL")
    sql, _ = connection.cursor().executed[-1]
    assert "activity_type = 'CALL'" in sql


def test_read_cursor_rejects_an_injected_activity_type():
    with pytest.raises(ValueError, match="activity type"):
        read_cursor(FakeConnection([]), "dev", "hubspot", "CALL' or 1=1 --")


@pytest.mark.parametrize("bad", ["has space", "has-dash", "has`tick", "drop;table", ""])
def test_validate_identifier_rejects_anything_that_is_not_a_bare_identifier(bad):
    with pytest.raises(ValueError, match="catalog"):
        validate_identifier("catalog", bad)


def test_format_cursor_ts_normalizes_to_naive_utc_microseconds():
    assert format_cursor_ts(datetime(2026, 8, 1, 9, 0, 0)) == "2026-08-01 09:00:00.000000"
    assert format_cursor_ts("2026-08-01T09:00:00") == "2026-08-01 09:00:00.000000"


def test_the_landing_relation_is_fully_qualified():
    assert landing_relation("raw", "hubspot") == "`raw`.`hubspot`.`companion_activity_contact_associations`"


def test_the_landing_relation_rejects_an_injected_schema():
    with pytest.raises(ValueError, match="schema"):
        landing_relation("raw", "hubspot; drop table x")


# --- end to end ------------------------------------------------------------


def test_extract_lands_a_snapshot_per_activity_and_resumes_from_the_worklist():
    worklist = [("101", datetime(2026, 11, 1)), ("102", datetime(2026, 11, 2))]
    connection = FakeConnection(worklist)
    session = FakeSession([FakeResponse(body=_fixture("associations_linked.json"))])
    insert = MagicMock()
    config = AssociationExtractionConfig(
        catalog="dev",
        schema="hubspot",
        source_prefix="raw.hubspot",
        api_token="tok",
        dag_run_id="run-1",
        extracted_at="2026-11-24T00:00:00",
    )

    with (
        pytest.MonkeyPatch.context() as mp,
    ):
        mp.setattr("include.custom_functions.hubspot_api.insert_rows", insert)
        summary = extract_activity_associations(connection, config, "calls", "CALL", session=session)

    assert summary["activity_type"] == "CALL"
    assert summary["activities_read"] == 2
    # The recorded fixture links 101 only; 102 is a real row with an empty array.
    assert summary["activities_with_links"] == 1
    assert summary["activities_without_links"] == 1
    assert summary["cursor_activity_id"] == "102"
    assert summary["cursor_source_changed_at"] == "2026-11-02 00:00:00.000000"

    landed = insert.call_args[0][3]
    by_id = {row.activity_id: row for row in landed}
    assert json.loads(by_id["101"].contact_ids_json) == ["9001", "9002"]
    assert by_id["102"].contact_ids_json == "[]"


def test_extract_of_an_empty_worklist_writes_nothing_and_keeps_the_cursor():
    connection = FakeConnection([])
    session = FakeSession([])
    insert = MagicMock()
    config = AssociationExtractionConfig(
        catalog="dev",
        schema="hubspot",
        source_prefix="raw.hubspot",
        api_token="tok",
        dag_run_id="run-1",
        extracted_at="2026-11-24T00:00:00",
        full_sweep=True,
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("include.custom_functions.hubspot_api.insert_rows", insert)
        summary = extract_activity_associations(connection, config, "notes", "NOTE", session=session)

    assert summary["activities_read"] == 0
    assert summary["batches"] == 0
    # Neither half of the cursor is reported from a run that read nothing.
    assert summary["cursor_activity_id"] is None
    assert summary["cursor_source_changed_at"] is None
    # Nothing was read, so nothing is written: an empty INSERT would still commit a table.
    insert.assert_not_called()


def test_a_full_sweep_does_not_read_a_cursor():
    connection = FakeConnection([])
    session = FakeSession([])
    config = AssociationExtractionConfig(
        catalog="dev",
        schema="hubspot",
        source_prefix="raw.hubspot",
        api_token="tok",
        dag_run_id="run-1",
        extracted_at="2026-11-24T00:00:00",
        full_sweep=True,
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("include.custom_functions.hubspot_api.insert_rows", MagicMock())
        extract_activity_associations(connection, config, "tasks", "TASK", session=session)

    executed = [sql for sql, _ in connection.cursor().executed]
    assert not any("ORDER BY source_changed_at DESC" in sql for sql in executed)
