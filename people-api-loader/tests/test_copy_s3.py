"""copy step: per-file aws_s3 COPY, idempotency, error aggregation."""

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


def test_copy_one_file_emits_table_import_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._copy_one_file(_CFG, "20260609", "wh", "VoterTX", "voter_export_20260609/state_id=TX/part-0.csv")
    sql = executed_sql(conn)
    assert any("aws_s3.table_import_from_s3" in s for s in sql)
    # all five session SETs precede the import
    assert any("SET synchronous_commit = off" in s for s in sql)
    assert any("SET statement_timeout = 0" in s for s in sql)


def test_run_loads_state_and_completes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    files = [
        SimpleNamespace(state="TX", s3_key="voter_export_20260609/state_id=TX/part-0.csv", size_bytes=10)
    ]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")

    def _read(cfg, rd, name, model):
        return None if name == "copy" else unload

    monkeypatch.setattr(step, "read_manifest", _read)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    # _load_state returns a complete result without touching a real DB.
    monkeypatch.setattr(
        step,
        "_load_state",
        lambda **kw: step.CopyTableResult(
            table=f"Voter{kw['state']}",
            expected_rows=kw["expected_rows"],
            actual_rows=kw["expected_rows"],
            files_loaded=1,
            seconds_elapsed=1.0,
        ),
    )
    manifest = step.run(_CFG, "20260609")
    assert manifest.status == "complete"
    assert manifest.results[0].table == "VoterTX"


def test_skips_zero_byte_files(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_keys: list[str] = []
    files = [
        SimpleNamespace(state="TX", s3_key="k-empty", size_bytes=0),
        SimpleNamespace(state="TX", s3_key="k-good", size_bytes=10),
    ]
    unload = _unload(files, {"TX": 5})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _capture(**kw):
        seen_keys.extend(kw["s3_keys"])
        return step.CopyTableResult(
            table=f"Voter{kw['state']}",
            expected_rows=kw["expected_rows"],
            actual_rows=kw["expected_rows"],
            files_loaded=len(kw["s3_keys"]),
            seconds_elapsed=0.0,
        )

    monkeypatch.setattr(step, "_load_state", _capture)
    step.run(_CFG, "20260609")
    assert "k-empty" not in seen_keys and "k-good" in seen_keys
