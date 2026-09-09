from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from retl import cli, databricks_io
from retl.cli import build_parser, main
from retl.destinations import RowError
from retl.run import RunSummary
from retl.sent_log import FLOW_ID_PROPERTY
from tests._fakes import FakeConnection, stamped_table

LOG_TABLE = "goodparty_data_catalog.reverse_etl.sent_log_hubspot_leads"
CSV_ENV = {
    "RETL_FLOW_HUBSPOT_LEADS_SOURCE_RELATION": "goodparty_data_catalog.mart_sales_reverse_etl.contact_desired_state",
    "RETL_FLOW_HUBSPOT_LEADS_KEY_COLUMN": "gp_person_id",
    "RETL_FLOW_HUBSPOT_LEADS_CAP": "10",
    "RETL_FLOW_HUBSPOT_LEADS_LOG_TABLE": LOG_TABLE,
    "DATABRICKS_HOST": "dbc-example.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
    "DATABRICKS_TOKEN": "test-token",
}


def test_build_parser_rejects_an_unknown_destination() -> None:
    """Catches: a typo'd --destination value silently doing nothing instead of failing at parse time."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--source", "hubspot_leads", "--destination", "not-a-real-destination"])


def test_build_parser_requires_source_and_a_mode() -> None:
    """Catches: retl running with no flow, and no destination or --init-log selected."""
    with pytest.raises(SystemExit):
        build_parser().parse_args([])
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--source", "hubspot_leads"])


def test_build_parser_rejects_init_log_and_destination_together() -> None:
    """Catches: --init-log and --destination both accepted, which would try to run a diff
    and create a table in one confused invocation instead of exactly one action."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--source", "hubspot_leads", "--destination", "csv", "--init-log"])


def test_parse_args_rejects_accept_empty_log_with_init_log() -> None:
    """Catches: --accept-empty-log accepted alongside --init-log, where it means nothing --
    --init-log never reads the log's row count at all."""
    with pytest.raises(SystemExit):
        cli.parse_args(["--source", "hubspot_leads", "--init-log", "--accept-empty-log"])


def test_main_runs_a_csv_preview_end_to_end(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Catches: a break anywhere in the source-to-destination wiring for the simplest real
    path -- a destination that returned without writing anything would still pass a
    stdout-only assertion, so this checks the file the run was actually supposed to produce."""
    csv_path = tmp_path / "preview.csv"
    env = {**CSV_ENV, "RETL_CSV_OUTPUT_PATH": str(csv_path)}
    monkeypatch.setattr(os, "environ", env)
    fake_connection = FakeConnection(
        source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}],
        tables={
            LOG_TABLE: stamped_table(
                "hubspot_leads",
                rows=[
                    {"tracking_key": "existing", "payload": "{}", "sent_at": datetime(2026, 1, 1, tzinfo=UTC)}
                ],
            )
        },
    )
    monkeypatch.setattr(databricks_io, "connect", lambda _config: fake_connection)

    exit_code = main(["--source", "hubspot_leads", "--destination", "csv"])

    assert exit_code == 0
    assert fake_connection.closed is True
    captured = capsys.readouterr()
    assert "retl flow=hubspot_leads" in captured.out
    assert captured.err == ""  # a clean run has no error detail to print
    assert csv_path.read_text().splitlines() == [
        "tracking_key,payload",
        'p1,"{""firstname"":""Jane"",""gp_person_id"":""p1""}"',
    ]


def test_main_prints_error_codes_to_stderr_when_rows_are_rejected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Catches: row-level error codes never reaching process output, so a wrapping DAG
    task's failure alert would carry only a bare exit code."""
    env = {**CSV_ENV, "RETL_CSV_OUTPUT_PATH": str(tmp_path / "preview.csv")}
    monkeypatch.setattr(os, "environ", env)
    monkeypatch.setattr(databricks_io, "connect", lambda _config: FakeConnection())
    summary = RunSummary(
        flow_id="hubspot_leads",
        source_count=1,
        to_send_count=1,
        sent_count=0,
        error_count=1,
        orphaned_key_count=0,
        errors=[RowError("hubspot_leads", "p1", "VALIDATION_ERROR", "phone", False)],
    )
    monkeypatch.setattr(cli, "execute_run", lambda **_kwargs: summary)

    exit_code = main(["--source", "hubspot_leads", "--destination", "csv"])

    assert exit_code == 1
    assert "code=VALIDATION_ERROR" in capsys.readouterr().err


def test_main_fails_without_reaching_databricks_when_flow_config_is_missing(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Catches: retl attempting to connect before its own config is known to be valid."""
    monkeypatch.setattr("os.environ", {})
    connect_calls: list[object] = []
    monkeypatch.setattr(databricks_io, "connect", lambda config: connect_calls.append(config))

    exit_code = main(["--source", "hubspot_leads", "--destination", "csv"])

    assert exit_code == 1
    assert connect_calls == []
    assert "retl FAILED" in capsys.readouterr().err


def test_main_init_log_creates_the_table_and_exits_zero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Catches: --init-log not actually calling init_log_table, or reusing the daily run
    path instead of the dedicated setup ceremony."""
    monkeypatch.setattr(os, "environ", CSV_ENV)
    fake_connection = FakeConnection()
    monkeypatch.setattr(databricks_io, "connect", lambda _config: fake_connection)

    exit_code = main(["--source", "hubspot_leads", "--init-log"])

    assert exit_code == 0
    assert fake_connection.closed is True
    assert fake_connection.tables[LOG_TABLE].properties == {FLOW_ID_PROPERTY: "hubspot_leads"}
    captured = capsys.readouterr()
    assert "created" in captured.out
    assert captured.err == ""


def test_main_init_log_reports_already_present_on_a_second_call(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Catches: init losing its idempotent "already present" report, which is the only
    signal an operator gets that re-running init was a no-op rather than a fresh create."""
    monkeypatch.setattr(os, "environ", CSV_ENV)
    fake_connection = FakeConnection(tables={LOG_TABLE: stamped_table("hubspot_leads")})
    monkeypatch.setattr(databricks_io, "connect", lambda _config: fake_connection)

    exit_code = main(["--source", "hubspot_leads", "--init-log"])

    assert exit_code == 0
    assert "already present" in capsys.readouterr().out
