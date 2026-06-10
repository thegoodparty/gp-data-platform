"""copy: per-file aws_s3 import into public."Voter", State-keyed idempotency."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import copy_s3 as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b", aws_region="us-west-2"))


def _unload(files, counts):
    return SimpleNamespace(status="complete", files=files, per_state_row_counts=counts)


def test_copy_one_file_targets_voter_with_session_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._copy_one_file(_CFG, "20260609", "wh", "voter_export_20260609/state_id=TX/part-0.csv")
    sql = executed_sql(conn)
    assert any("aws_s3.table_import_from_s3" in s for s in sql)
    # all five session SETs run before the import
    for setting in (
        "synchronous_commit",
        "maintenance_work_mem",
        "work_mem",
        "statement_timeout",
        "idle_in_transaction_session_timeout",
    ):
        assert any(f"SET {setting}" in s for s in sql), f"missing SET {setting}"
    # the import targets the single unified table, not a per-state table
    import_params = next(p for s, p in conn.executed if "table_import_from_s3" in s)
    assert import_params["table"] == 'public."Voter"'


def test_load_state_skips_when_count_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((100,))  # existing rows == expected
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    r = step._load_state(
        cfg=_CFG,
        run_date="20260609",
        writer_endpoint="wh",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
    )
    assert r.files_loaded == 0 and r.state == "TX" and r.table == "Voter"


def test_run_completes_and_records_state(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(
        step,
        "_load_state",
        lambda **kw: step.CopyTableResult(
            table="Voter",
            state=kw["state"],
            expected_rows=kw["expected_rows"],
            actual_rows=kw["expected_rows"],
            files_loaded=1,
            seconds_elapsed=1.0,
        ),
    )
    manifest = step.run(_CFG, "20260609")
    assert manifest.status == "complete"
    assert manifest.results[0].state == "TX"
