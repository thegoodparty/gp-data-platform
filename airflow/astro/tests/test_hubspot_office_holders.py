"""Contract tests for the Office Holder companion extractor.

Everything here is a recorded fixture shaped like the HubSpot CRM API answers. No test
calls HubSpot, and none needs the loader token.
"""

import json
from unittest.mock import Mock

import pytest
import requests
from include.custom_functions.hubspot_office_holders import (
    ASSOCIATION_BATCH_SIZE,
    ASSOCIATION_DIRECTIONS,
    INSERT_COLUMNS,
    LANDING_COLUMNS,
    OBJECT_TYPE,
    OFFICE_HOLDER_PROPERTIES,
    REQUESTED_PROPERTIES,
    RUNS_INSERT_COLUMNS,
    SYSTEM_PROPERTIES,
    HubSpotError,
    HubSpotSweepError,
    _landed_row_fields,
    _row_values,
    attempt_run_id,
    build_landing_rows,
    extract_office_holders,
    fetch_links,
    insert_landing_rows,
    linked_ids,
    list_records,
    request_json,
    write_completion_marker,
)

TOKEN = "test-token"


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None):
        self.status_code = status_code
        self._body = body if body is not None else {}
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeSession:
    """Returns queued responses in order and records every request it was given.

    A queued Exception instance is raised instead of returned, so a network-level failure
    can be simulated on one call.
    """

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []

    def request(self, method, url, params=None, json=None, headers=None, timeout=None):
        self.requests.append(
            {
                "method": method,
                "url": url,
                "params": params,
                "json": json,
                "headers": headers,
                "timeout": timeout,
            }
        )
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _page(records, after=None):
    body = {"results": records}
    if after:
        body["paging"] = {"next": {"after": after}}
    return FakeResponse(body=body)


def _record(record_id, properties=None):
    return {"id": str(record_id), "properties": properties or {}}


# --- the property list -------------------------------------------------------------------


def test_the_request_names_all_31_custom_properties():
    """A property omitted from the request returns a null column, not an error, so the
    count is the only cheap guard that the list is whole."""
    assert len(OFFICE_HOLDER_PROPERTIES) == 31
    assert len(set(OFFICE_HOLDER_PROPERTIES)) == 31


def test_the_two_unique_keys_and_the_display_name_are_requested():
    for name in ("gp_api_elected_office_id", "gp_elected_official_term_id", "name"):
        assert name in OFFICE_HOLDER_PROPERTIES


def test_every_requested_property_is_also_a_landing_column():
    columns = {name for name, _ in LANDING_COLUMNS}
    for name in REQUESTED_PROPERTIES:
        assert name in columns, f"{name} is requested but has no landing column"


def test_the_merge_and_date_properties_are_always_requested():
    """The model's create and update columns and its only merge signal."""
    for name in ("hs_createdate", "hs_lastmodifieddate", "hubspot_owner_id", "hs_merged_object_ids"):
        assert name in SYSTEM_PROPERTIES
        assert name in REQUESTED_PROPERTIES


def test_the_landing_row_fields_line_up_with_the_insert_columns():
    """A field that stopped matching its column would bind every later value one off."""
    assert _landed_row_fields() == INSERT_COLUMNS


def test_the_runs_table_binds_every_column_it_declares():
    runs_columns = {
        "run_id",
        "started_at",
        "completed_at",
        "record_count",
        "contact_link_count",
        "company_link_count",
    }
    assert set(RUNS_INSERT_COLUMNS) == runs_columns - {"completed_at"}


# --- the list sweep ----------------------------------------------------------------------


def test_list_records_pages_on_after_until_the_cursor_stops():
    session = FakeSession(
        [
            _page([_record(1), _record(2)], after="cursor-1"),
            _page([_record(3)], after="cursor-2"),
            _page([_record(4)]),
        ]
    )

    records = list(list_records(session, TOKEN, page_limit=2))

    assert [record["id"] for record in records] == ["1", "2", "3", "4"]
    assert session.requests[0]["params"].get("after") is None
    assert session.requests[1]["params"]["after"] == "cursor-1"
    assert session.requests[2]["params"]["after"] == "cursor-2"


def test_list_records_asks_for_every_property_by_name():
    session = FakeSession([_page([])])

    list(list_records(session, TOKEN))

    requested = session.requests[0]["params"]["properties"].split(",")
    assert requested == list(REQUESTED_PROPERTIES)


def test_list_records_uses_the_list_endpoint_not_search():
    """Search stops at 10,000 results; the feed will add volume past that."""
    session = FakeSession([_page([])])

    list(list_records(session, TOKEN))

    assert session.requests[0]["method"] == "GET"
    assert session.requests[0]["url"].endswith(f"/crm/v3/objects/{OBJECT_TYPE}")
    assert "search" not in session.requests[0]["url"]


def test_list_records_an_empty_page_ends_the_sweep():
    session = FakeSession([_page([])])

    assert list(list_records(session, TOKEN)) == []
    assert len(session.requests) == 1


def test_list_records_fails_rather_than_looping_forever_when_paging_never_ends():
    session = FakeSession([_page([_record(1)], after=f"cursor-{i}") for i in range(5)])

    with pytest.raises(HubSpotSweepError, match="still paging"):
        list(list_records(session, TOKEN, max_pages=3))


# --- link handling ----------------------------------------------------------------------


def test_a_link_carrying_two_types_yields_one_id():
    """The app writes type 76 and HubSpot adds the unlabeled 71 for the same pair. A count
    of type entries would double every such link."""
    body = {
        "results": [
            {
                "from": {"id": "900"},
                "to": [
                    {
                        "toObjectId": 149656527718,
                        "associationTypes": [
                            {"category": "USER_DEFINED", "typeId": 76, "label": "Office Held"},
                            {"category": "HUBSPOT_DEFINED", "typeId": 71, "label": None},
                        ],
                    }
                ],
            }
        ]
    }

    assert linked_ids(body) == {"900": ["149656527718"]}


def test_a_link_carrying_only_the_unlabeled_type_is_kept():
    """A typeId filter would drop this one, which is the failure the design avoids."""
    body = {
        "results": [
            {
                "from": {"id": "900"},
                "to": [
                    {
                        "toObjectId": 4242,
                        "associationTypes": [{"category": "HUBSPOT_DEFINED", "typeId": 71, "label": None}],
                    }
                ],
            }
        ]
    }

    assert linked_ids(body) == {"900": ["4242"]}


def test_a_record_absent_from_the_association_response_gets_an_explicit_empty_array():
    """An empty array is a claim that the record has no links; a missing row would read as
    a record that does not exist."""
    rows = build_landing_rows(
        records=[_record(900, {"name": "Mayor (OR)"})],
        links={"contacts": {}, "companies": {}},
        run_id="run-1",
        extracted_at="2026-11-24T09:30:00Z",
    )

    assert len(rows) == 1
    assert json.loads(rows[0].contact_ids) == []
    assert json.loads(rows[0].company_ids) == []


def test_a_response_with_an_empty_to_list_yields_an_explicit_empty_array():
    body = {"results": [{"from": {"id": "900"}, "to": []}]}

    assert linked_ids(body) == {"900": []}


def test_fetch_links_groups_at_the_batch_boundary():
    record_ids = [str(i) for i in range(ASSOCIATION_BATCH_SIZE + 1)]
    session = FakeSession([FakeResponse(body={"results": []}), FakeResponse(body={"results": []})])

    fetch_links(session, TOKEN, "contacts", record_ids)

    assert [len(request["json"]["inputs"]) for request in session.requests] == [ASSOCIATION_BATCH_SIZE, 1]


def test_fetch_links_takes_every_direction_from_one_batch_call_each():
    session = FakeSession([FakeResponse(body={"results": []})])

    fetch_links(session, TOKEN, "companies", ["900", "901"])

    assert len(session.requests) == 1
    assert session.requests[0]["url"].endswith(f"/crm/v4/associations/{OBJECT_TYPE}/companies/batch/read")
    assert session.requests[0]["json"] == {"inputs": [{"id": "900"}, {"id": "901"}]}


def test_fetch_links_never_sends_a_type_filter():
    session = FakeSession([FakeResponse(body={"results": []})])

    fetch_links(session, TOKEN, "contacts", ["900"])

    assert "typeId" not in json.dumps(session.requests[0]["json"])


def test_fetch_links_fails_the_run_when_a_batch_reports_an_error():
    """An empty array per unresolved id would claim no links, which a failed batch does not
    know. Failing means no completion marker, so the previous snapshot stays in force."""
    session = FakeSession(
        [
            FakeResponse(
                status_code=207,
                body={
                    "results": [{"from": {"id": "900"}, "to": []}],
                    "errors": [{"category": "OBJECT_NOT_FOUND", "message": "record 901"}],
                },
            )
        ]
    )

    with pytest.raises(HubSpotSweepError, match="refusing to land a partial link set"):
        fetch_links(session, TOKEN, "contacts", ["900", "901"])


def test_fetch_links_rejects_an_unknown_direction():
    with pytest.raises(ValueError, match="unexpected association direction"):
        fetch_links(FakeSession([]), TOKEN, "deals", ["900"])


def test_both_directions_are_read():
    assert ASSOCIATION_DIRECTIONS == ("contacts", "companies")


def test_a_link_listed_twice_is_kept_once():
    body = {
        "results": [
            {"from": {"id": "900"}, "to": [{"toObjectId": 1, "associationTypes": []}]},
            {"from": {"id": "900"}, "to": [{"toObjectId": 1, "associationTypes": []}]},
        ]
    }

    assert linked_ids(body) == {"900": ["1"]}


# --- the landing rows -------------------------------------------------------------------


def test_each_property_lands_in_its_own_named_column():
    """office_email and office_phone have to be columns a source declaration can tag."""
    record = _record(
        900,
        {
            "name": "Mayor (OR)",
            "status": "in_office",
            "gp_api_elected_office_id": "abc",
            "office_email": "office@example.org",
            "office_phone": "555-0100",
        },
    )

    rows = build_landing_rows(
        [record], {"contacts": {"900": ["1"]}, "companies": {}}, "run-1", "2026-11-24T09:30:00Z"
    )
    bound = dict(zip(INSERT_COLUMNS, _row_values(rows[0]), strict=False))

    assert bound["name"] == "Mayor (OR)"
    assert bound["status"] == "in_office"
    assert bound["gp_api_elected_office_id"] == "abc"
    assert bound["office_email"] == "office@example.org"
    assert bound["office_phone"] == "555-0100"
    assert json.loads(bound["contact_ids"]) == ["1"]
    assert json.loads(bound["company_ids"]) == []


def test_a_property_hubspot_did_not_return_binds_as_null():
    rows = build_landing_rows(
        [_record(900)], {"contacts": {}, "companies": {}}, "run-1", "2026-11-24T09:30:00Z"
    )
    bound = dict(zip(INSERT_COLUMNS, _row_values(rows[0]), strict=False))

    assert bound["gp_elected_official_term_id"] is None
    assert bound["office_email"] is None


def test_a_listed_record_without_an_id_fails_the_sweep():
    with pytest.raises(HubSpotSweepError, match="carried no id"):
        build_landing_rows([{"properties": {}}], {}, "run-1", "2026-11-24T09:30:00Z")


# --- the HTTP client --------------------------------------------------------------------


def test_a_429_is_retried_then_the_request_succeeds():
    session = FakeSession(
        [
            FakeResponse(status_code=429, headers={"Retry-After": "0"}),
            FakeResponse(body={"ok": True}),
        ]
    )

    body = request_json(session, "GET", "/crm/v3/objects/x", TOKEN, sleep=lambda seconds: None)

    assert body == {"ok": True}
    assert len(session.requests) == 2


def test_a_500_is_retried():
    session = FakeSession([FakeResponse(status_code=503), FakeResponse(body={"ok": True})])

    assert request_json(session, "GET", "/p", TOKEN, sleep=lambda seconds: None) == {"ok": True}


def test_a_400_is_not_retried():
    session = FakeSession([FakeResponse(status_code=400, body={"category": "BAD", "message": "nope"})])

    with pytest.raises(HubSpotError, match="400"):
        request_json(session, "GET", "/p", TOKEN, sleep=lambda seconds: None)

    assert len(session.requests) == 1


def test_the_token_is_stripped_before_it_reaches_the_header():
    session = FakeSession([FakeResponse(body={})])

    request_json(session, "GET", "/p", f"  {TOKEN}\n", sleep=lambda seconds: None)

    assert session.requests[0]["headers"]["Authorization"] == f"Bearer {TOKEN}"


def test_a_blank_token_is_refused_before_any_request():
    session = FakeSession([])

    with pytest.raises(HubSpotError, match="empty or missing"):
        request_json(session, "GET", "/p", "   ", sleep=lambda seconds: None)

    assert session.requests == []


def test_a_network_error_never_puts_the_token_in_the_message():
    session = FakeSession([requests.exceptions.ConnectionError(f"bad header {TOKEN}") for _ in range(6)])

    with pytest.raises(HubSpotError) as excinfo:
        request_json(session, "GET", "/p", TOKEN, sleep=lambda seconds: None)

    assert TOKEN not in str(excinfo.value)


# --- the marker and the sweep -----------------------------------------------------------


class FakeCursor:
    def __init__(self, recorded):
        self.recorded = recorded

    def execute(self, query, parameters=None):
        self.recorded.append((query, parameters))

    def close(self):
        pass


class FakeConnection:
    def __init__(self):
        self.recorded = []

    def cursor(self):
        return FakeCursor(self.recorded)


def _statements(connection):
    return [(query, parameters) for query, parameters in connection.recorded]


def _is_marker_insert(query):
    """The completion marker write, as opposed to the CREATE that names the same table."""
    return query.startswith("INSERT INTO `cat`.`sch`.`companion_office_holders_runs`")


def test_the_marker_is_written_after_every_record_row():
    """The ordering is the whole guard: staging reads only runs with a marker."""
    connection = FakeConnection()
    order = []

    original_insert = insert_landing_rows

    def recording_insert(*args, **kwargs):
        order.append("rows")
        return original_insert(*args, **kwargs)

    def recording_marker(*args, **kwargs):
        order.append("marker")
        return write_completion_marker(*args, **kwargs)

    import include.custom_functions.hubspot_office_holders as module

    original_insert_landing = module.insert_landing_rows
    original_marker = module.write_completion_marker
    module.insert_landing_rows = recording_insert
    module.write_completion_marker = recording_marker
    try:
        session = FakeSession(
            [
                _page([_record(900, {"name": "Mayor (OR)"})]),
                FakeResponse(body={"results": [{"from": {"id": "900"}, "to": []}]}),
                FakeResponse(body={"results": [{"from": {"id": "900"}, "to": []}]}),
            ]
        )
        extract_office_holders(connection, session, TOKEN, "cat", "sch", "run-1")
    finally:
        module.insert_landing_rows = original_insert_landing
        module.write_completion_marker = original_marker

    assert order == ["rows", "marker"]


def test_a_retried_attempt_lands_under_its_own_run_id():
    """Airflow retries a task under the same dag run. Landing a retry under the dag run id
    would append a second copy of every record under the id whose marker it then writes, so
    the readable snapshot would hold each record twice."""
    first = attempt_run_id({"dag_run": Mock(run_id="scheduled__1"), "ti": Mock(try_number=1)})
    retried = attempt_run_id({"dag_run": Mock(run_id="scheduled__1"), "ti": Mock(try_number=2)})

    assert first == "scheduled__1.1"
    assert retried == "scheduled__1.2"
    assert first != retried


def test_the_attempt_run_id_defaults_to_one_when_no_task_instance_is_in_context():
    assert attempt_run_id({"dag_run": Mock(run_id="manual__1")}) == "manual__1.1"


def test_the_second_attempts_snapshot_supersedes_the_firsts():
    """A retry after a failed chunk writes its own marker, and that is the run staging reads."""
    connection = FakeConnection()
    session = FakeSession(
        [
            _page([_record(900)]),
            FakeResponse(body={"results": []}),
            FakeResponse(body={"results": []}),
        ]
    )

    extract_office_holders(connection, session, TOKEN, "cat", "sch", "scheduled__1.2")

    statements = _statements(connection)
    marker = next(parameters for query, parameters in statements if _is_marker_insert(query))
    assert marker["run_id_0"] == "scheduled__1.2"


def test_a_sweep_that_errors_before_the_marker_writes_no_marker():
    connection = FakeConnection()
    session = FakeSession(
        [
            _page([_record(900)]),
            FakeResponse(status_code=207, body={"results": [], "errors": [{"category": "BAD"}]}),
        ]
    )

    with pytest.raises(HubSpotSweepError):
        extract_office_holders(connection, session, TOKEN, "cat", "sch", "run-1")

    assert not any(_is_marker_insert(query) for query, _ in _statements(connection))


def test_an_empty_sweep_is_refused_rather_than_marked_complete():
    connection = FakeConnection()
    session = FakeSession([_page([])])

    with pytest.raises(HubSpotSweepError, match="no records"):
        extract_office_holders(connection, session, TOKEN, "cat", "sch", "run-1")

    assert not any(_is_marker_insert(query) for query, _ in _statements(connection))


def test_a_dry_run_writes_nothing_at_all():
    connection = FakeConnection()
    session = FakeSession(
        [
            _page([_record(900)]),
            FakeResponse(body={"results": []}),
            FakeResponse(body={"results": []}),
        ]
    )

    result = extract_office_holders(connection, session, TOKEN, "cat", "sch", "run-1", dry_run=True)

    assert result["record_count"] == 1
    assert result["dry_run"] is True
    assert _statements(connection) == []


def test_the_landing_statement_derives_its_columns_from_the_declared_columns():
    connection = FakeConnection()
    rows = build_landing_rows(
        [_record(900)], {"contacts": {}, "companies": {}}, "run-1", "2026-11-24T09:30:00Z"
    )

    insert_landing_rows(connection, "cat", "sch", rows)

    query, parameters = _statements(connection)[0]
    # Every column is named, including the server-set loaded_at, so one list still drives
    # the DDL, the INSERT column list and the bound placeholders.
    every_column = ", ".join(name for name, _ in LANDING_COLUMNS)
    assert query.startswith(f"INSERT INTO `cat`.`sch`.`companion_office_holders` ({every_column})")
    assert "current_timestamp()" in query
    assert set(parameters) == {f"{column}_0" for column in _landed_row_fields()}


def test_the_marker_carries_the_run_counts():
    connection = FakeConnection()

    write_completion_marker(connection, "cat", "sch", "run-1", "2026-11-24T09:30:00Z", 3051, 3040, 3000)

    query, parameters = _statements(connection)[0]
    assert "companion_office_holders_runs" in query
    assert parameters["run_id_0"] == "run-1"
    assert parameters["record_count_0"] == 3051
    assert parameters["contact_link_count_0"] == 3040
    assert parameters["company_link_count_0"] == 3000
