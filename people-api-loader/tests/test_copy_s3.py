"""copy: per-file aws_s3 import into public."Voter", State-keyed idempotency."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import copy_s3 as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b", aws_region="us-west-2"))

# Minimal Voter DDL with a mid-table lowercase Prisma column, so column extraction
# (and ordering) is exercised by the run() tests without the full prod snapshot.
_DDL = (
    'CREATE TABLE public."Voter" (\n'
    '    "LALVOTERID" text NOT NULL,\n'
    '    "State" text NOT NULL,\n'
    "    id uuid NOT NULL\n"
    ");"
)
_COLS = '"LALVOTERID", "State", "id"'


def _unload(files, counts):
    return SimpleNamespace(status="complete", files=files, per_state_row_counts=counts)


def test_copy_one_file_targets_voter_with_session_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._copy_one_file(_CFG, "20260609", "wh", "voter_export_20260609/state_id=TX/part-0.csv", _COLS)
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
    # the import targets the single unified table, not a per-state table, and passes the
    # explicit column list (not '') so the load isn't a bare positional map.
    import_params = next(p for s, p in conn.executed if "table_import_from_s3" in s)
    assert import_params["table"] == 'public."Voter"'
    assert import_params["columns"] == _COLS


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
        column_list=_COLS,
    )
    assert r.files_loaded == 0 and r.state == "TX" and r.table == "Voter"


def test_run_completes_and_records_state(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DDL)
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


def _passthrough_load_state(**kw):
    return step.CopyTableResult(
        table="Voter",
        state=kw["state"],
        expected_rows=kw["expected_rows"],
        actual_rows=kw["expected_rows"],
        files_loaded=1,
        seconds_elapsed=1.0,
    )


def test_full_run_raises_and_writes_no_manifest_when_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    # CA is expected (count>0) but has no loadable files -> a full run can't cover it.
    # It must raise (surface the anomaly) and NOT persist an in_progress manifest.
    wrote: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100, "CA": 200})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "_load_state", _passthrough_load_state)
    with pytest.raises(RuntimeError, match="copy incomplete"):
        step.run(_CFG, "20260609")
    assert "m" not in wrote  # no manifest persisted on incomplete full run


def test_state_filter_partial_writes_no_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    # `--state TX` against a 2-state table loads TX only; the table isn't complete,
    # so no manifest is persisted and it does not raise.
    wrote: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100, "CA": 200})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "_load_state", _passthrough_load_state)
    manifest = step.run(_CFG, "20260609", state_filter="TX")
    assert "m" not in wrote  # not persisted
    assert manifest.status == "in_progress"


def test_load_state_partial_reload_deletes_then_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    # pre-count 50 of expected 100 (partial) -> DELETE that state, reload; post-count 100.
    conn = FakeConn().queue_result((50,)).queue_result((100,))
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    r = step._load_state(
        cfg=_CFG,
        run_date="20260609",
        writer_endpoint="wh",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
        column_list=_COLS,
    )
    sql = executed_sql(conn)
    assert any('DELETE FROM public."Voter" WHERE "State"' in s for s in sql)
    assert r.files_loaded == 1 and r.actual_rows == 100


def test_load_state_skip_path_does_not_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    # exact match -> skip, and crucially no destructive DELETE fires.
    conn = FakeConn().queue_result((100,))
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._load_state(
        cfg=_CFG,
        run_date="20260609",
        writer_endpoint="wh",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
        column_list=_COLS,
    )
    assert not any("DELETE FROM" in s for s in executed_sql(conn))


def test_state_filter_reloads_carried_partial_state(monkeypatch: pytest.MonkeyPatch) -> None:
    # An in-progress manifest carried TX at 50/100. `--state TX` must reload it,
    # not silently skip it via the carry-forward.
    called: dict = {}
    existing = SimpleNamespace(
        status="in_progress",
        results=[
            step.CopyTableResult(
                table="Voter",
                state="TX",
                expected_rows=100,
                actual_rows=50,
                files_loaded=2,
                seconds_elapsed=1.0,
            )
        ],
    )
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: existing if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _ls(**kw):
        called["state"] = kw["state"]
        return step.CopyTableResult(
            table="Voter",
            state=kw["state"],
            expected_rows=kw["expected_rows"],
            actual_rows=kw["expected_rows"],
            files_loaded=1,
            seconds_elapsed=1.0,
        )

    monkeypatch.setattr(step, "_load_state", _ls)
    step.run(_CFG, "20260609", state_filter="TX")
    assert called.get("state") == "TX"


def test_state_filter_reloads_fully_loaded_state(monkeypatch: pytest.MonkeyPatch) -> None:
    # TX was fully loaded (100/100) in a prior in-progress run. `--state TX` must
    # still reload it — the carry-forward must not skip an explicitly-targeted state.
    called: dict = {}
    existing = SimpleNamespace(
        status="in_progress",
        results=[
            step.CopyTableResult(
                table="Voter",
                state="TX",
                expected_rows=100,
                actual_rows=100,
                files_loaded=3,
                seconds_elapsed=1.0,
            )
        ],
    )
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: existing if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _ls(**kw):
        called["state"] = kw["state"]
        return step.CopyTableResult(
            table="Voter",
            state=kw["state"],
            expected_rows=kw["expected_rows"],
            actual_rows=kw["expected_rows"],
            files_loaded=1,
            seconds_elapsed=1.0,
        )

    monkeypatch.setattr(step, "_load_state", _ls)
    step.run(_CFG, "20260609", state_filter="TX")
    assert called.get("state") == "TX"


def test_state_filter_no_loadable_files_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # --state TX but every TX file is zero-byte (filtered out) -> must raise, not silently no-op.
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=0)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    with pytest.raises(RuntimeError, match="no loadable"):
        step.run(_CFG, "20260609", state_filter="TX")


def test_load_state_acquires_advisory_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    # The per-state advisory lock must be taken (before the count) so a concurrent
    # invocation can't read 0 and double-load.
    conn = FakeConn().queue_result((100,))  # already complete -> skip, after the lock
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._load_state(
        cfg=_CFG,
        run_date="20260609",
        writer_endpoint="wh",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
        column_list=_COLS,
    )
    # Assert ordering, not just presence: the lock must precede the row count, else a
    # reordering regression (count first) would defeat the guard yet still pass.
    sql = executed_sql(conn)
    lock_idx = next(i for i, s in enumerate(sql) if "pg_advisory_lock" in s)
    count_idx = next(i for i, s in enumerate(sql) if "count(*)" in s)
    assert lock_idx < count_idx, "advisory lock must be acquired before the row count"
