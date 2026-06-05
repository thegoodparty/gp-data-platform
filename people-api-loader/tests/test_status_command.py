"""Regression tests for the `status` command's row rendering.

These cover two delegate-reviewer findings on PR #1:
  - a manifest with `status='failed'` must render distinctly, not as
    `in_progress`;
  - a manifest that cannot be read (S3 down, bad creds, corrupt body) must
    surface as an explicit error row, not collapse into the `—` not-run marker.

No AWS or env is touched: `_setup` is patched to a fake config and
`read_manifest` is patched to drive each scenario. Rows are captured by
swapping in a recording `Table`, so assertions hit the exact status-cell
markup rather than Rich's width-dependent rendered output.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from loader.people_api.cli import app

runner = CliRunner()


def _invoke_status(monkeypatch, read_manifest_impl):
    """Run `status --date ...` with read_manifest stubbed; return captured rows."""
    monkeypatch.setattr(
        "loader.people_api.cli._setup",
        lambda run_date, *, verify_aws=True: SimpleNamespace(
            s3_bucket="test-bucket",
            manifest_key=lambda rd, name: f"voter_export_{rd}/_manifest/{name}.json",
        ),
    )
    monkeypatch.setattr("loader.core.manifest.io.read_manifest", read_manifest_impl)

    captured: list = []

    class _RecordingTable:
        def __init__(self, *args, **kwargs):
            self.rows: list[tuple] = []
            captured.append(self)

        def add_column(self, *args, **kwargs):
            return None

        def add_row(self, *cols):
            self.rows.append(cols)

    monkeypatch.setattr("loader.people_api.cli.Table", _RecordingTable)

    result = runner.invoke(app, ["status", "--date", "20260101"])
    assert result.exit_code == 0, result.output
    assert captured, "status command did not build a table"
    return captured[0].rows


@pytest.mark.parametrize(
    ("status", "expected_cell"),
    [
        ("complete", "[green]complete[/green]"),
        ("in_progress", "[yellow]in_progress[/yellow]"),
        ("failed", "[red]failed[/red]"),
    ],
)
def test_status_renders_each_manifest_status(monkeypatch, status, expected_cell) -> None:
    def fake_read(cfg, run_date, step, model):
        return SimpleNamespace(status=status)

    rows = _invoke_status(monkeypatch, fake_read)

    assert rows, "expected one row per step"
    # Every step shares the same stubbed status, so the status column must be
    # exactly the expected markup — in particular `failed` is never folded into
    # the `in_progress` bucket.
    assert {row[1] for row in rows} == {expected_cell}


def test_status_reports_unreadable_manifest_as_error(monkeypatch) -> None:
    def fake_read(cfg, run_date, step, model):
        raise RuntimeError("S3 unreachable")

    rows = _invoke_status(monkeypatch, fake_read)

    assert rows, "expected one row per step"
    for _name, status_cell, detail in rows:
        assert status_cell == "[red]error[/red]"
        assert "S3 unreachable" in detail
    # The failure must not masquerade as the "not yet run" marker.
    assert all(row[1] != "—" for row in rows)
