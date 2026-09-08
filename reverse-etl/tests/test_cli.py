from __future__ import annotations

import os
from pathlib import Path

import pytest

from retl import cli, databricks_io
from retl.cli import build_parser, main
from retl.destinations import RowError
from retl.run import RunSummary
from tests._fakes import FakeConnection

CSV_ENV = {
    "RETL_FLOW_HUBSPOT_LEADS_SOURCE_RELATION": "goodparty_data_catalog.mart_sales_reverse_etl.contact_desired_state",
    "RETL_FLOW_HUBSPOT_LEADS_KEY_COLUMN": "gp_person_id",
    "RETL_FLOW_HUBSPOT_LEADS_CAP": "10",
    "RETL_LOG_TABLE": "goodparty_data_catalog.reverse_etl.sent_log",
    "DATABRICKS_HOST": "dbc-example.cloud.databricks.com",
    "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
    "DATABRICKS_TOKEN": "test-token",
}


def test_build_parser_rejects_an_unknown_destination() -> None:
    """Catches: a typo'd --destination value silently doing nothing instead of failing at parse time."""
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--source", "hubspot_leads", "--destination", "not-a-real-destination"])


def test_build_parser_requires_source_and_destination() -> None:
    """Catches: retl running with no flow or destination selected."""
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


def test_main_runs_a_csv_preview_end_to_end(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Catches: a break anywhere in the source-to-destination wiring for the simplest real path."""
    env = {**CSV_ENV, "RETL_CSV_OUTPUT_PATH": str(tmp_path / "preview.csv")}
    monkeypatch.setattr(os, "environ", env)
    fake_connection = FakeConnection(source_rows=[{"gp_person_id": "p1", "firstname": "Jane"}])
    monkeypatch.setattr(databricks_io, "connect", lambda _config: fake_connection)

    exit_code = main(["--source", "hubspot_leads", "--destination", "csv"])

    assert exit_code == 0
    assert fake_connection.closed is True
    captured = capsys.readouterr()
    assert "retl flow=hubspot_leads" in captured.out
    assert captured.err == ""  # a clean run has no error detail to print


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
