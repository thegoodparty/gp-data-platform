from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pytest

from retl.config import FlowConfig
from retl.destinations import DeliveryResult, RowError
from retl.run import (
    EmptySourceError,
    InvalidTrackingKeyError,
    SendCapExceededError,
    error_report_lines,
    execute_run,
)
from tests._fakes import FakeConnection

FLOW = FlowConfig(
    flow_id="hubspot_leads",
    source_relation="goodparty_data_catalog.mart_sales_reverse_etl.contact_desired_state",
    key_column="gp_person_id",
    excluded_columns=frozenset({"added_to_mart_at"}),
    cap=10,
)
LOG_TABLE = "goodparty_data_catalog.reverse_etl.sent_log"


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


def test_execute_run_sends_a_brand_new_person_and_logs_the_confirmation() -> None:
    """Catches: a new person never reaching the destination, or a confirmed send never being logged."""
    connection = FakeConnection(
        source_rows=[{"gp_person_id": "p1", "firstname": "Jane", "added_to_mart_at": "x"}]
    )
    destination = _FakeDestination()

    summary = execute_run(connection=connection, flow=FLOW, log_table=LOG_TABLE, destination=destination)

    assert summary.sent_count == 1
    assert connection.log_table[0]["tracking_key"] == "p1"
    assert connection.log_table[0]["payload"] == '{"firstname":"Jane","gp_person_id":"p1"}'


def test_execute_run_sends_nothing_when_payload_is_unchanged() -> None:
    """Catches: a steady-state day re-sending someone whose payload already matches the log."""
    connection = FakeConnection(
        source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}],
        log_table=[
            {
                "flow_id": "hubspot_leads",
                "tracking_key": "p1",
                "payload": '{"firstname":"Jane","gp_person_id":"p1"}',
                "sent_at": datetime(2026, 1, 1, tzinfo=UTC),
            }
        ],
    )
    destination = _FakeDestination()

    summary = execute_run(connection=connection, flow=FLOW, log_table=LOG_TABLE, destination=destination)

    assert summary.to_send_count == 0
    assert summary.sent_count == 0
    assert destination.delivered == [("hubspot_leads", [])]


def test_execute_run_raises_empty_source_error_on_zero_source_rows() -> None:
    """Catches: a broken upstream join reading as a quiet (empty-diff) day instead of failing loud."""
    connection = FakeConnection(source_rows=[])
    with pytest.raises(EmptySourceError):
        execute_run(connection=connection, flow=FLOW, log_table=LOG_TABLE, destination=_FakeDestination())


def test_execute_run_raises_and_sends_nothing_when_the_diff_exceeds_the_cap() -> None:
    """Catches: a runaway diff sending a partial batch before the cap is checked; overflow must mean zero sends."""
    connection = FakeConnection(
        source_rows=[{"gp_person_id": f"p{i}", "firstname": "Jane"} for i in range(FLOW.cap + 1)]
    )
    destination = _RejectingDestination()

    with pytest.raises(SendCapExceededError):
        execute_run(connection=connection, flow=FLOW, log_table=LOG_TABLE, destination=destination)

    assert destination.called is False
    assert connection.log_table == []


def test_execute_run_counts_a_logged_key_no_longer_in_the_source_as_orphaned() -> None:
    """Catches: a person-id remint going unnoticed in the run summary, which is the watch
    until an id-mismatch hold on the model side can arm."""
    connection = FakeConnection(
        source_rows=[{"gp_person_id": "new_id", "firstname": "Jane"}],
        log_table=[
            {
                "flow_id": "hubspot_leads",
                "tracking_key": "old_id",
                "payload": '{"firstname":"Jane"}',
                "sent_at": datetime(2026, 1, 1, tzinfo=UTC),
            }
        ],
    )
    summary = execute_run(
        connection=connection, flow=FLOW, log_table=LOG_TABLE, destination=_FakeDestination()
    )

    assert summary.orphaned_key_count == 1


def test_execute_run_reports_rejected_rows_in_the_summary_without_logging_them() -> None:
    """Catches: a rejected row being logged as sent, which would mean it never gets retried
    the next day even though HubSpot never actually accepted it."""
    connection = FakeConnection(source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}])
    summary = execute_run(
        connection=connection, flow=FLOW, log_table=LOG_TABLE, destination=_RejectingDestination()
    )
    assert summary.error_count == 1
    assert connection.log_table == []


@pytest.mark.parametrize(
    "source_rows",
    [
        [{"gp_person_id": None, "firstname": "Jane"}],
        [{"gp_person_id": "   ", "firstname": "Jane"}],
        [{"gp_person_id": "p1", "firstname": "Jane"}, {"gp_person_id": "p1", "firstname": "Bob"}],
    ],
    ids=["null_key", "blank_key", "duplicate_key"],
)
def test_execute_run_raises_on_an_invalid_tracking_key_and_sends_nothing(
    source_rows: list[dict[str, Any]],
) -> None:
    """Catches: a null key silently becoming the string 'None' and being sent/logged (so the
    corrupt row never retries), a blank key doing the same, or a duplicate key silently
    last-write-winning instead of failing the run loud before any guard or POST."""
    connection = FakeConnection(source_rows=source_rows)
    destination = _RejectingDestination()

    with pytest.raises(InvalidTrackingKeyError):
        execute_run(connection=connection, flow=FLOW, log_table=LOG_TABLE, destination=destination)

    assert destination.called is False
    assert connection.log_table == []


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
