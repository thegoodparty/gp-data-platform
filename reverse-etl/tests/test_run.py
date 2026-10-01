from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

import pytest

from retl.config import FlowConfig
from retl.destinations import DeliveryResult, RowError
from retl.hubspot_destination import chunked
from retl.run import (
    EmptyLogError,
    EmptySourceError,
    InvalidTrackingKeyError,
    SendCapExceededError,
    error_report_lines,
    execute_run,
    plan_run,
)
from retl.sent_log import PARAM_CHAR_LIMIT, WrongLogTableError
from tests._fakes import FakeConnection, flow_tables, stamped_table

LOG_TABLE = "goodparty_data_catalog.reverse_etl.sent_log_hubspot_leads"
FLOW = FlowConfig(
    flow_id="hubspot_leads",
    source_relation="goodparty_data_catalog.mart_sales_reverse_etl.contact_desired_state",
    key_column="gp_person_id",
    excluded_columns=frozenset({"added_to_mart_at"}),
    cap=10,
    log_table=LOG_TABLE,
)


@dataclass
class _FakeDestination:
    """Confirms everything it is handed, calling on_batch_confirmed once per delivered row set."""

    delivered: list[tuple[str, list[tuple[str, str]]]] = field(default_factory=list)

    def deliver(self, flow_id, rows, *, on_batch_confirmed):
        self.delivered.append((flow_id, list(rows)))
        confirmed = dict(rows)
        on_batch_confirmed(confirmed)
        return DeliveryResult(confirmed=confirmed, errors=[])


@dataclass
class _RejectingDestination:
    """Confirms nothing; used to prove a guard failure never reaches delivery."""

    called: bool = False

    def deliver(self, flow_id, rows, *, on_batch_confirmed):
        self.called = True
        return DeliveryResult(confirmed={}, errors=[RowError(flow_id, None, "REJECTED", None, False)])


_EXISTING_ROW = {
    "tracking_key": "existing",
    "payload": '{"firstname":"Prior"}',
    "sent_at": datetime(2025, 1, 1, tzinfo=UTC),
}


def _connection_with_log(rows: list[dict[str, Any]] | None = None, **kwargs: Any) -> FakeConnection:
    """A FakeConnection whose flow's log table already exists, correctly stamped, and
    (unless a test overrides `rows`) already has history: most tests here are not
    testing the empty-log guard, so they should not need to think about it. The two
    tests that are pass `rows=[]` explicitly.
    """
    seeded_rows = [_EXISTING_ROW] if rows is None else rows
    return FakeConnection(tables=flow_tables(FLOW.flow_id, LOG_TABLE, rows=seeded_rows), **kwargs)


def test_execute_run_sends_a_brand_new_person_and_logs_the_confirmation() -> None:
    """Catches: a new person never reaching the destination, or a confirmed send never being logged."""
    connection = _connection_with_log(
        source_rows=[{"gp_person_id": "p1", "firstname": "Jane", "added_to_mart_at": "x"}]
    )
    destination = _FakeDestination()

    summary = execute_run(connection=connection, flow=FLOW, destination=destination)

    assert summary.sent_count == 1
    appended = connection.tables[LOG_TABLE].rows[-1]  # index 0 is the fixture's pre-existing row
    assert appended["tracking_key"] == "p1"
    assert appended["payload"] == '{"firstname":"Jane","gp_person_id":"p1"}'


def test_execute_run_sends_nothing_when_payload_is_unchanged() -> None:
    """Catches: a steady-state day re-sending someone whose payload already matches the log."""
    connection = _connection_with_log(
        source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}],
        rows=[
            {
                "tracking_key": "p1",
                "payload": '{"firstname":"Jane","gp_person_id":"p1"}',
                "sent_at": datetime(2026, 1, 1, tzinfo=UTC),
            }
        ],
    )
    destination = _FakeDestination()

    summary = execute_run(connection=connection, flow=FLOW, destination=destination)

    assert summary.to_send_count == 0
    assert summary.sent_count == 0
    assert destination.delivered == [("hubspot_leads", [])]


def test_execute_run_raises_empty_source_error_on_zero_source_rows() -> None:
    """Catches: a broken upstream join reading as a quiet (empty-diff) day instead of failing loud."""
    connection = _connection_with_log(source_rows=[])
    with pytest.raises(EmptySourceError):
        execute_run(connection=connection, flow=FLOW, destination=_FakeDestination())


def test_execute_run_raises_and_sends_nothing_when_the_diff_exceeds_the_cap() -> None:
    """Catches: a runaway diff sending a partial batch before the cap is checked; overflow must mean zero sends."""
    connection = _connection_with_log(
        source_rows=[{"gp_person_id": f"p{i}", "firstname": "Jane"} for i in range(FLOW.cap + 1)]
    )
    destination = _RejectingDestination()

    with pytest.raises(SendCapExceededError):
        execute_run(connection=connection, flow=FLOW, destination=destination)

    assert destination.called is False
    assert connection.tables[LOG_TABLE].rows == [_EXISTING_ROW]  # unchanged, nothing appended


def test_execute_run_counts_a_logged_key_no_longer_in_the_source_as_orphaned() -> None:
    """Catches: a person-id remint going unnoticed in the run summary, which is the watch
    until an id-mismatch hold on the model side can arm."""
    connection = _connection_with_log(
        source_rows=[{"gp_person_id": "new_id", "firstname": "Jane"}],
        rows=[
            {
                "tracking_key": "old_id",
                "payload": '{"firstname":"Jane"}',
                "sent_at": datetime(2026, 1, 1, tzinfo=UTC),
            }
        ],
    )
    summary = execute_run(connection=connection, flow=FLOW, destination=_FakeDestination())

    assert summary.orphaned_key_count == 1


def test_execute_run_reports_rejected_rows_in_the_summary_without_logging_them() -> None:
    """Catches: a rejected row being logged as sent, which would mean it never gets retried
    the next day even though HubSpot never actually accepted it."""
    connection = _connection_with_log(source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}])
    summary = execute_run(connection=connection, flow=FLOW, destination=_RejectingDestination())
    assert summary.error_count == 1
    assert connection.tables[LOG_TABLE].rows == [_EXISTING_ROW]  # unchanged, nothing appended


@pytest.mark.parametrize(
    "source_rows",
    [
        [{"gp_person_id": None, "firstname": "Jane"}],
        [{"gp_person_id": "   ", "firstname": "Jane"}],
        [{"gp_person_id": "p1", "firstname": "Jane"}, {"gp_person_id": "p1", "firstname": "Bob"}],
        [{"person_id": "p1", "firstname": "Jane"}],
    ],
    ids=["null_key", "blank_key", "duplicate_key", "missing_key_column"],
)
def test_execute_run_raises_on_an_invalid_tracking_key_and_sends_nothing(
    source_rows: list[dict[str, Any]],
) -> None:
    """Catches: a null key silently becoming the string 'None' and being sent/logged (so the
    corrupt row never retries), a blank key doing the same, or a duplicate key silently
    last-write-winning instead of failing the run loud before any guard or POST."""
    connection = _connection_with_log(rows=[], source_rows=source_rows)
    destination = _RejectingDestination()

    with pytest.raises(InvalidTrackingKeyError):
        execute_run(connection=connection, flow=FLOW, destination=destination)

    assert destination.called is False
    assert connection.tables[LOG_TABLE].rows == []


def test_execute_run_raises_empty_log_error_without_the_accept_flag() -> None:
    """Catches: an amnesia day (a lost/recreated/truncated table, or a mis-pointed log_table
    config reading zero rows for this flow) silently re-sending the full population instead
    of failing loud -- the cap alone cannot catch it since a legitimate recompute day also
    rewrites most rows."""
    connection = _connection_with_log(rows=[], source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}])
    destination = _RejectingDestination()

    with pytest.raises(EmptyLogError):
        execute_run(connection=connection, flow=FLOW, destination=destination)

    assert destination.called is False
    assert connection.tables[LOG_TABLE].rows == []


def test_execute_run_accept_empty_log_permits_a_deliberate_first_run() -> None:
    """Catches: the amnesia guard blocking the one day a genuinely empty log is expected --
    a first run, or a run right after an admin reset."""
    connection = _connection_with_log(rows=[], source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}])
    destination = _FakeDestination()

    summary = execute_run(connection=connection, flow=FLOW, destination=destination, accept_empty_log=True)

    assert summary.sent_count == 1


def test_execute_run_raises_wrong_log_table_error_for_a_misconfigured_log_table() -> None:
    """Catches: a flow's RETL_FLOW_<NAME>_LOG_TABLE pointing at ANOTHER flow's (stamped)
    table -- reading it would see that flow's rows as this flow's latest_sent (non-empty,
    so the empty-log guard alone could never catch it), full-resend this flow's population,
    and corrupt both flows' histories by writing into the wrong table."""
    connection = FakeConnection(
        source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}],
        tables={LOG_TABLE: stamped_table("techspeed_leads")},  # wrong flow's stamp
    )
    destination = _RejectingDestination()

    with pytest.raises(WrongLogTableError):
        execute_run(connection=connection, flow=FLOW, destination=destination)

    assert destination.called is False
    assert connection.tables[LOG_TABLE].rows == []


def test_execute_run_issues_no_ddl_and_a_missing_table_fails_the_run() -> None:
    """Catches: the daily run path ever creating or altering a table (that ceremony belongs
    only to --init-log), and a lost log table reading as a quiet empty day instead of
    failing loud."""
    connection = FakeConnection(source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}])  # no tables at all
    destination = _RejectingDestination()

    with pytest.raises(Exception, match="no such table"):
        execute_run(connection=connection, flow=FLOW, destination=destination)

    assert destination.called is False
    assert not any(sql.lower().startswith("create table") for sql in connection.executed_sql)


def test_error_report_lines_is_empty_for_a_clean_run() -> None:
    """Catches: a clean run printing extra noise it has nothing to say."""
    assert error_report_lines([]) == []


def test_error_report_lines_groups_a_histogram_then_lists_each_row() -> None:
    """Catches: row-level error codes never reaching process output, or the histogram
    miscounting a group."""
    errors = [
        RowError("hubspot_leads", "p1", "VALIDATION_ERROR", "phone", False),
        RowError("hubspot_leads", "p2", "VALIDATION_ERROR", "phone", False),
        RowError("hubspot_leads", "p3", "RATE_LIMITED", None, True),
    ]
    assert error_report_lines(errors) == [
        "retl error_histogram code=RATE_LIMITED property=None retryable=True count=1",
        "retl error_histogram code=VALIDATION_ERROR property=phone retryable=False count=2",
        "retl error flow=hubspot_leads tracking_key=p1 code=VALIDATION_ERROR property=phone retryable=False",
        "retl error flow=hubspot_leads tracking_key=p2 code=VALIDATION_ERROR property=phone retryable=False",
        "retl error flow=hubspot_leads tracking_key=p3 code=RATE_LIMITED property=None retryable=True",
    ]


def test_error_report_lines_caps_individual_rows_at_20() -> None:
    """Catches: a systemically-rejected convergence day dumping tens of thousands of lines."""
    errors = [RowError("hubspot_leads", f"p{i}", "VALIDATION_ERROR", "phone", False) for i in range(25)]

    lines = error_report_lines(errors)

    assert lines[0] == "retl error_histogram code=VALIDATION_ERROR property=phone retryable=False count=25"
    individual_lines = [line for line in lines if line.startswith("retl error flow=")]
    assert len(individual_lines) == 20


@dataclass
class _BatchingDestination:
    """Confirms in HubSpot-sized batches, optionally raising after `fail_after` batches."""

    fail_after: int | None = None

    def deliver(self, flow_id, rows, *, on_batch_confirmed):
        confirmed: dict[str, str] = {}
        for n, batch in enumerate(chunked(rows)):
            if n == self.fail_after:
                raise RuntimeError("destination went away mid-run")
            on_batch_confirmed(dict(batch))
            confirmed.update(batch)
        return DeliveryResult(confirmed=confirmed, errors=[])


def test_execute_run_packs_many_confirmed_batches_into_full_log_chunks() -> None:
    """Catches: one log write per 100-row destination batch, or partly filled chunks. Each
    write is its own Delta commit (~2.5s live), so a 100k-row convergence logged batch by
    batch would spend ~40 minutes committing."""
    rows = [{"gp_person_id": f"p{i}", "firstname": "J" * 900} for i in range(5_000)]
    connection = _connection_with_log(source_rows=rows)

    summary = execute_run(
        connection=connection, flow=replace(FLOW, cap=20_000), destination=_BatchingDestination()
    )

    assert summary.sent_count == 5_000
    assert len(connection.tables[LOG_TABLE].rows) == 1 + 5_000
    inserts = [sql for sql in connection.executed_sql if sql.startswith(f"insert into {LOG_TABLE} ")]
    assert 1 < len(inserts) <= 6
    assert all(size > PARAM_CHAR_LIMIT * 0.85 for size in connection.param_sizes[-len(inserts) : -1])


def test_execute_run_logs_every_confirmed_batch_when_delivery_raises_mid_run() -> None:
    """Catches: buffered confirmations being dropped when the destination fails partway,
    so rows the destination already accepted would be resent on the next run."""
    rows = [{"gp_person_id": f"p{i}", "firstname": "Jane"} for i in range(1_000)]
    connection = _connection_with_log(source_rows=rows)

    with pytest.raises(RuntimeError, match="went away"):
        execute_run(
            connection=connection,
            flow=replace(FLOW, cap=20_000),
            destination=_BatchingDestination(fail_after=3),
        )

    assert len(connection.tables[LOG_TABLE].rows) == 1 + 300


def test_plan_run_dry_run_summary_reports_the_diff_and_touches_nothing() -> None:
    """Catches: a dry run writing to the log, or reporting a different diff than the real run would send."""
    connection = _connection_with_log(source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}])

    summary = plan_run(connection=connection, flow=FLOW).dry_run_summary()

    assert (summary.to_send_count, summary.sent_count, summary.dry_run) == (1, 0, True)
    assert connection.tables[LOG_TABLE].rows == [_EXISTING_ROW]


def test_plan_run_enforces_the_guards() -> None:
    """Catches: a dry run skipping the cap or empty-log guards, so it reports success for a
    run that would in fact be refused."""
    rows = [{"gp_person_id": f"p{i}"} for i in range(FLOW.cap + 1)]
    with pytest.raises(SendCapExceededError):
        plan_run(connection=_connection_with_log(source_rows=rows), flow=FLOW)
    with pytest.raises(EmptyLogError):
        plan_run(connection=_connection_with_log(rows=[], source_rows=rows[:1]), flow=FLOW)


ORPHANS = f"{LOG_TABLE}_orphans"
_JANE = '{"firstname":"Jane","gp_person_id":"p1"}'


def _events(connection: FakeConnection) -> list[tuple[str, str, str]]:
    return [(r["tracking_key"], r["event"], r["last_payload"]) for r in connection.tables[ORPHANS].rows]


def _sent_p1_before() -> list[dict[str, Any]]:
    return [{"tracking_key": "p1", "payload": _JANE, "sent_at": datetime(2025, 1, 1, tzinfo=UTC)}]


def test_execute_run_records_a_key_that_left_the_source_once_with_its_last_payload() -> None:
    """Catches: a deleted row going unrecorded (nothing to clean up by hand), a missing
    event without the payload needed to find the contact, or a duplicate event every run."""
    connection = _connection_with_log(
        rows=_sent_p1_before(), source_rows=[{"gp_person_id": "p2", "firstname": "Sam"}]
    )

    first = execute_run(connection=connection, flow=FLOW, destination=_FakeDestination())
    execute_run(connection=connection, flow=FLOW, destination=_FakeDestination())

    assert (first.orphaned_key_count, first.newly_missing_count) == (1, 1)
    assert _events(connection) == [("p1", "missing", _JANE)]


def test_execute_run_resends_a_returned_key_once_even_when_its_payload_is_unchanged() -> None:
    """Catches: a contact deleted by hand while its key was missing never being recreated
    when the person comes back unchanged -- the diff alone sees the old payload in the log
    and sends nothing."""
    connection = _connection_with_log(
        rows=_sent_p1_before(), source_rows=[{"gp_person_id": "p2", "firstname": "Sam"}]
    )
    execute_run(connection=connection, flow=FLOW, destination=_FakeDestination())
    connection.source_rows = [
        {"gp_person_id": "p1", "firstname": "Jane"},
        {"gp_person_id": "p2", "firstname": "Sam"},
    ]

    destination = _FakeDestination()
    back = execute_run(connection=connection, flow=FLOW, destination=destination)
    again = execute_run(connection=connection, flow=FLOW, destination=_FakeDestination())

    assert ("p1", _JANE) in destination.delivered[0][1]
    assert back.returned_count == 1
    assert _events(connection) == [("p1", "missing", _JANE), ("p1", "returned", _JANE)]
    assert again.to_send_count == 0


def test_execute_run_keeps_a_returned_key_open_when_its_resend_is_rejected() -> None:
    """Catches: a return being recorded before the destination accepted the resend, so a
    rejected resend would never be retried and the contact would stay deleted."""
    connection = _connection_with_log(
        rows=_sent_p1_before(), source_rows=[{"gp_person_id": "p2", "firstname": "Sam"}]
    )
    execute_run(connection=connection, flow=FLOW, destination=_FakeDestination())
    connection.source_rows = [
        {"gp_person_id": "p1", "firstname": "Jane"},
        {"gp_person_id": "p2", "firstname": "Sam"},
    ]

    execute_run(connection=connection, flow=FLOW, destination=_RejectingDestination())
    retry = plan_run(connection=connection, flow=FLOW)

    assert [e[1] for e in _events(connection)] == ["missing"]
    assert ("p1", _JANE) in retry.to_send


def test_plan_run_reports_orphan_changes_without_writing_them() -> None:
    """Catches: a dry run recording orphan events, which would mark a key as handled
    (or as returned) on a run that sent nothing."""
    connection = _connection_with_log(
        rows=_sent_p1_before(), source_rows=[{"gp_person_id": "p2", "firstname": "Sam"}]
    )

    summary = plan_run(connection=connection, flow=FLOW).dry_run_summary()

    assert summary.newly_missing_count == 1
    assert connection.tables[ORPHANS].rows == []


def test_plan_run_refuses_an_orphans_table_stamped_for_another_flow() -> None:
    """Catches: a flow reading another flow's open orphans as its own and force-resending them."""
    connection = _connection_with_log(source_rows=[{"gp_person_id": "p1"}])
    connection.tables[ORPHANS] = stamped_table("techspeed_leads")

    with pytest.raises(WrongLogTableError):
        plan_run(connection=connection, flow=FLOW)
