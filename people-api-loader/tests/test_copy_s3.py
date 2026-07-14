"""copy: per-file aws_s3 import per table, per-(table, unit) idempotency.

A "unit" is a state for a partitioned table (Voter, DistrictVoter) or the single ""
unit for a flat table (District, DistrictStats) — mirrors UnloadFile/UnloadTable's
state="" convention for flat tables.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import psycopg
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
_OPTS = "(FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8')"

# Minimal flat-table (District) DDL, for run()-level multi-table tests.
_DISTRICT_DDL = 'CREATE TABLE public."District" (\n    id text NOT NULL,\n    "Name" text NOT NULL\n);'
_DISTRICT_COLS = '"id", "Name"'

# Combined DDL feeding load_target_schema() when a run() test loads more than one table.
_DDL_MULTI = _DDL + "\n" + _DISTRICT_DDL


def _unload(files, counts):
    # Single-table (Voter) unload manifest shape: copy iterates unload.tables, so a
    # single-entry list exercises the Voter-only regression path.
    voter = SimpleNamespace(table="Voter", files=files, row_counts=counts)
    return SimpleNamespace(status="complete", tables=[voter])


def _unload_multi(*table_entries):
    tables = [
        SimpleNamespace(table=table, files=files, row_counts=counts) for table, files, counts in table_entries
    ]
    return SimpleNamespace(status="complete", tables=tables)


# A realistic mix of target types: text columns (which legitimately hold empty
# strings) and typed columns (which cannot — an empty field must import as NULL).
_MIXED_TYPES = {
    "LALVOTERID": "TEXT",
    "State": "TEXT",
    "Age_Int": "INTEGER",
    "Active": "BOOLEAN",
    "Estimated_Income": "DOUBLE PRECISION",
    "created_at": "TIMESTAMPTZ",
    "Registration_Date": "DATE",
    "id": "UUID",
}


def test_force_null_columns_selects_only_non_text() -> None:
    # Text columns keep their empty-vs-NULL distinction (empty string preserved);
    # every non-text type is FORCE_NULL so a quoted-empty "" imports as NULL.
    forced = step._force_null_columns(_MIXED_TYPES)
    assert forced == ["Age_Int", "Active", "Estimated_Income", "created_at", "Registration_Date", "id"]
    assert "LALVOTERID" not in forced and "State" not in forced


def test_import_options_appends_force_null_for_typed_columns() -> None:
    opts = step._import_options(["Age_Int", "Active"])
    # base CSV options are preserved verbatim (still paired with unload)...
    assert opts.startswith("(FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8'")
    # ...and a FORCE_NULL clause for the named typed columns is appended inside the parens.
    assert opts.endswith(', FORCE_NULL ("Age_Int", "Active"))')


def test_import_options_no_force_null_when_all_text() -> None:
    # No typed columns -> plain base options, no dangling FORCE_NULL ().
    assert step._import_options([]) == step._IMPORT_OPTIONS
    assert "FORCE_NULL" not in step._import_options([])


def test_copy_one_file_targets_given_table_with_session_sets(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    opts = "(FORMAT csv, DELIMITER E'\\t', NULL '', QUOTE '\"', ESCAPE '\"', ENCODING 'UTF8')"
    step._copy_one_file(
        _CFG, "20260609", "voter_export_20260609/state_id=TX/part-0.csv", "Voter", _COLS, opts
    )
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
    # the import targets the table passed in, not a hardcoded one, and passes the
    # explicit column list (not '') so the load isn't a bare positional map.
    import_params = next(p for s, p in conn.executed if "table_import_from_s3" in s)
    assert import_params["table"] == 'public."Voter"'
    assert import_params["columns"] == _COLS
    # The caller-built options string is passed straight through to the import.
    assert import_params["options"] == opts


def test_copy_one_file_targets_a_different_table(monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression guard: the target table must come from the `table` param, not a
    # module-level constant — a second table proves it isn't hardcoded to "Voter".
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._copy_one_file(
        _CFG, "20260609", "district_export/data/part-0.csv", "District", _DISTRICT_COLS, _OPTS
    )
    import_params = next(p for s, p in conn.executed if "table_import_from_s3" in s)
    assert import_params["table"] == 'public."District"'
    assert import_params["columns"] == _DISTRICT_COLS


def test_acquire_unit_lock_key_differs_by_table_for_same_state() -> None:
    # Two tables loading the same state must NOT share a lock key, or a same-state
    # load of Voter and DistrictVoter could interleave unsafely.
    conn = FakeConn()
    cur = cast(psycopg.Cursor, conn.cursor())
    step._acquire_unit_lock(cur, "Voter", "TX")
    step._acquire_unit_lock(cur, "DistrictVoter", "TX")
    lock_calls = [(sql, params) for sql, params in conn.executed if "pg_advisory_lock" in sql]
    assert len(lock_calls) == 2
    (_, voter_params), (_, district_voter_params) = lock_calls
    assert voter_params != district_voter_params
    # hashtext is fed a table-qualified key, not the bare state.
    assert voter_params[1] == "Voter:TX"
    assert district_voter_params[1] == "DistrictVoter:TX"


def test_load_unit_skips_when_count_matches(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((100,))  # existing rows == expected
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    r = step._load_unit(
        cfg=_CFG,
        run_date="20260609",
        table="Voter",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
        column_list=_COLS,
        options=_OPTS,
    )
    assert r.files_loaded == 0 and r.state == "TX" and r.table == "Voter"


def test_load_unit_partial_reload_deletes_then_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    # pre-count 50 of expected 100 (partial) -> DELETE that state, reload; post-count 100.
    conn = FakeConn().queue_result((50,)).queue_result((100,))
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    r = step._load_unit(
        cfg=_CFG,
        run_date="20260609",
        table="Voter",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
        column_list=_COLS,
        options=_OPTS,
    )
    sql = executed_sql(conn)
    assert any('DELETE FROM public."Voter" WHERE "State"' in s for s in sql)
    assert r.files_loaded == 1 and r.actual_rows == 100


def test_load_unit_skip_path_does_not_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    # exact match -> skip, and crucially no destructive DELETE fires.
    conn = FakeConn().queue_result((100,))
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._load_unit(
        cfg=_CFG,
        run_date="20260609",
        table="Voter",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
        column_list=_COLS,
        options=_OPTS,
    )
    assert not any("DELETE FROM" in s for s in executed_sql(conn))


def test_load_unit_acquires_advisory_lock(monkeypatch: pytest.MonkeyPatch) -> None:
    # The per-unit advisory lock must be taken (before the count) so a concurrent
    # invocation can't read 0 and double-load.
    conn = FakeConn().queue_result((100,))  # already complete -> skip, after the lock
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._load_unit(
        cfg=_CFG,
        run_date="20260609",
        table="Voter",
        state="TX",
        expected_rows=100,
        s3_keys=["k"],
        parallelism=1,
        column_list=_COLS,
        options=_OPTS,
    )
    # Assert ordering, not just presence: the lock must precede the row count, else a
    # reordering regression (count first) would defeat the guard yet still pass.
    sql = executed_sql(conn)
    lock_idx = next(i for i, s in enumerate(sql) if "pg_advisory_lock" in s)
    count_idx = next(i for i, s in enumerate(sql) if "count(*)" in s)
    assert lock_idx < count_idx, "advisory lock must be acquired before the row count"
    # The int4 cast is required for the (int4, int4) overload — a regression that drops
    # it picks the nonexistent (bigint, int4) overload and fails only against real PG.
    assert "::int4" in sql[lock_idx]


def test_load_unit_flat_table_whole_table_count_no_where_state(monkeypatch: pytest.MonkeyPatch) -> None:
    # A flat table's unit is state="" — idempotency is whole-table, not `WHERE "State"`.
    conn = FakeConn().queue_result((0,)).queue_result((50,))  # pre-count 0 -> load; post-count 50
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    r = step._load_unit(
        cfg=_CFG,
        run_date="20260609",
        table="District",
        state="",
        expected_rows=50,
        s3_keys=["k"],
        parallelism=1,
        column_list=_DISTRICT_COLS,
        options=_OPTS,
    )
    sql = executed_sql(conn)
    count_stmts = [s for s in sql if "count(*)" in s]
    assert count_stmts and all("WHERE" not in s for s in count_stmts)
    assert all('public."District"' in s for s in count_stmts)
    assert r == step.CopyTableResult(
        table="District",
        state="",
        expected_rows=50,
        actual_rows=50,
        files_loaded=1,
        seconds_elapsed=r.seconds_elapsed,
    )


def test_load_unit_flat_table_partial_reload_deletes_whole_table(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((20,)).queue_result((50,))  # partial -> whole-table delete, reload
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._load_unit(
        cfg=_CFG,
        run_date="20260609",
        table="District",
        state="",
        expected_rows=50,
        s3_keys=["k"],
        parallelism=1,
        column_list=_DISTRICT_COLS,
        options=_OPTS,
    )
    sql = executed_sql(conn)
    delete_stmts = [s for s in sql if s.startswith("DELETE FROM")]
    assert delete_stmts == ['DELETE FROM public."District"']  # no WHERE "State" clause


def test_load_unit_flat_table_skip_path_does_not_delete(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((50,))  # exact match -> skip
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    r = step._load_unit(
        cfg=_CFG,
        run_date="20260609",
        table="District",
        state="",
        expected_rows=50,
        s3_keys=["k"],
        parallelism=1,
        column_list=_DISTRICT_COLS,
        options=_OPTS,
    )
    assert not any("DELETE FROM" in s for s in executed_sql(conn))
    assert r.files_loaded == 0 and r.table == "District" and r.state == ""


def _passthrough_load_unit(**kw):
    return step.CopyTableResult(
        table=kw["table"],
        state=kw["state"],
        expected_rows=kw["expected_rows"],
        actual_rows=kw["expected_rows"],
        files_loaded=1,
        seconds_elapsed=1.0,
    )


def test_run_completes_and_records_state(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "_load_unit", _passthrough_load_unit)
    manifest = step.run(_CFG, "20260609")
    assert manifest.status == "complete"
    assert manifest.results[0].state == "TX"
    assert manifest.results[0].table == "Voter"


def test_run_loads_voter_per_state_and_district_whole_table(monkeypatch: pytest.MonkeyPatch) -> None:
    # Regression + new behavior in one: Voter (partitioned) still loads per-state,
    # AND a flat table (District) in the same run loads as a single "" unit.
    calls: list[tuple[str, str]] = []

    def _lu(**kw):
        calls.append((kw["table"], kw["state"]))
        return _passthrough_load_unit(**kw)

    voter_files = [SimpleNamespace(state="TX", s3_key="voter/state=TX/part-0.csv", size_bytes=10)]
    district_files = [SimpleNamespace(state="", s3_key="district/data/part-0.csv", size_bytes=10)]
    unload = _unload_multi(
        ("Voter", voter_files, {"TX": 100}),
        ("District", district_files, {"": 5}),
    )
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL_MULTI)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "_load_unit", _lu)

    manifest = step.run(_CFG, "20260609")

    assert manifest.status == "complete"
    assert ("Voter", "TX") in calls
    assert ("District", "") in calls
    result_by_table = {r.table: r for r in manifest.results}
    assert result_by_table["Voter"].state == "TX"
    assert result_by_table["District"].state == ""


def test_state_filter_skips_flat_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    # `--state TX` only applies to partitioned tables; a flat table has no "TX" unit
    # to load, so it must be skipped entirely (not raise, not get a "" load attempt).
    calls: list[tuple[str, str]] = []

    def _lu(**kw):
        calls.append((kw["table"], kw["state"]))
        return _passthrough_load_unit(**kw)

    voter_files = [SimpleNamespace(state="TX", s3_key="voter/state=TX/part-0.csv", size_bytes=10)]
    district_files = [SimpleNamespace(state="", s3_key="district/data/part-0.csv", size_bytes=10)]
    unload = _unload_multi(
        ("Voter", voter_files, {"TX": 100, "CA": 50}),
        ("District", district_files, {"": 5}),
    )
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL_MULTI)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "_load_unit", _lu)

    manifest = step.run(_CFG, "20260609", state_filter="TX")

    assert calls == [("Voter", "TX")]  # District never touched
    assert manifest.status == "in_progress"


def test_full_run_raises_and_writes_no_manifest_when_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    # CA is expected (count>0) but has no loadable files -> a full run can't cover it.
    # It must raise (surface the anomaly) and NOT persist an in_progress manifest.
    wrote: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100, "CA": 200})
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "_load_unit", _passthrough_load_unit)
    with pytest.raises(RuntimeError, match="copy incomplete"):
        step.run(_CFG, "20260609")
    assert "m" not in wrote  # no manifest persisted on incomplete full run


def test_state_filter_partial_writes_no_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    # `--state TX` against a 2-state table loads TX only; the table isn't complete,
    # so no manifest is persisted and it does not raise.
    wrote: dict = {}
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=10)]
    unload = _unload(files, {"TX": 100, "CA": 200})
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "_load_unit", _passthrough_load_unit)
    manifest = step.run(_CFG, "20260609", state_filter="TX")
    assert "m" not in wrote  # not persisted
    assert manifest.status == "in_progress"


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
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: existing if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _lu(**kw):
        called["state"] = kw["state"]
        return _passthrough_load_unit(**kw)

    monkeypatch.setattr(step, "_load_unit", _lu)
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
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: existing if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")

    def _lu(**kw):
        called["state"] = kw["state"]
        return _passthrough_load_unit(**kw)

    monkeypatch.setattr(step, "_load_unit", _lu)
    step.run(_CFG, "20260609", state_filter="TX")
    assert called.get("state") == "TX"


def test_state_filter_no_loadable_files_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # --state TX but every TX file is zero-byte (filtered out) -> must raise, not silently no-op.
    files = [SimpleNamespace(state="TX", s3_key="state_id=TX/part-0.csv", size_bytes=0)]
    unload = _unload(files, {"TX": 100})
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DDL)
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "copy" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    with pytest.raises(RuntimeError, match="no loadable"):
        step.run(_CFG, "20260609", state_filter="TX")
