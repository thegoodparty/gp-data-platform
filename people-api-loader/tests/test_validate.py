"""validate: per-table count/schema/index gates.

Partitioned tables (Voter, DistrictVoter) get a per-state ±10% gate; flat tables (District,
DistrictStats) get a whole-table ±10% gate. Schema/index diffs run per table too. Sample-query
and l2Type-coverage checks stay Voter-only.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import UnloadTable, ValidateManifest
from loader.people_api.steps import validate as step
from tests._fakes import FakeConn, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))


def _unload_table(table: str, row_counts: dict[str, int]) -> UnloadTable:
    """A minimal `UnloadTable` for `_count_gate_check` tests (unused fields left at defaults)."""
    return cast(
        UnloadTable,
        SimpleNamespace(table=table, row_counts=row_counts, files=[], columns=[], column_types_pg={}),
    )


def _unload_all_tables() -> SimpleNamespace:
    """An UnloadManifest-shaped fixture spanning all four TABLE_SPECS tables."""
    return SimpleNamespace(
        status="complete",
        tables=[
            SimpleNamespace(table="Voter", row_counts={"TX": 100}),
            SimpleNamespace(table="District", row_counts={"": 50}),
            SimpleNamespace(table="DistrictStats", row_counts={"": 50}),
            SimpleNamespace(table="DistrictVoter", row_counts={"TX": 100}),
        ],
    )


# --- _compare_counts (per-state, partitioned tables) ---


def test_compare_counts_within_tolerance_pass() -> None:
    assert step._compare_counts("x", {"TX": 105}, {"TX": 100}).passed is True  # within ±10%


def test_compare_counts_outside_tolerance_fail() -> None:
    check = step._compare_counts("x", {"TX": 50}, {"TX": 100})  # 50 vs 100 → outside ±10%
    assert check.passed is False
    assert check.details["mismatch_count"] == 1


def test_compare_counts_expected_zero_requires_zero() -> None:
    assert step._compare_counts("x", {}, {"TX": 0}).passed is True
    assert step._compare_counts("x", {"TX": 1}, {"TX": 0}).passed is False


def test_compare_counts_flags_unexpected_state() -> None:
    # A state present in the table but not in the baseline must not pass silently.
    check = step._compare_counts("x", {"TX": 100, "ZZ": 5}, {"TX": 100})
    assert check.passed is False
    assert "ZZ" in check.details["mismatches"]


def test_compare_counts_flag_unexpected_false_ignores_new_state() -> None:
    assert step._compare_counts("x", {"TX": 100, "ZZ": 5}, {"TX": 100}, flag_unexpected=False).passed is True


# --- _compare_total (whole-table, flat tables) ---


def test_compare_total_within_tolerance_pass() -> None:
    assert step._compare_total("x", 105, 100).passed is True  # within ±10%


def test_compare_total_outside_tolerance_fail() -> None:
    check = step._compare_total("x", 50, 100)  # 50 vs 100 → outside ±10%
    assert check.passed is False
    assert check.details == {"expected": 100, "actual": 50, "tolerance": step._ROW_COUNT_TOLERANCE}
    # A flat-table gate compares a single total, not per-state details.
    assert "states" not in check.details
    assert "mismatches" not in check.details


def test_compare_total_expected_zero_requires_zero() -> None:
    assert step._compare_total("x", 0, 0).passed is True
    assert step._compare_total("x", 1, 0).passed is False


# --- _new_counts_by_state / _new_total_count ---


def test_new_counts_by_state(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result([("TX", 100), ("CA", 50)])
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    assert step._new_counts_by_state(_CFG, "20260609", "Voter") == {"TX": 100, "CA": 50}


def test_new_counts_by_state_drops_null_state(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result([("TX", 100), (None, 3)])  # NULL-State group must be dropped
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    assert step._new_counts_by_state(_CFG, "20260609", "DistrictVoter") == {"TX": 100}


def test_new_counts_by_state_queries_the_given_table(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result([("TX", 1)])
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._new_counts_by_state(_CFG, "20260609", "DistrictVoter")
    assert 'public."DistrictVoter"' in conn.executed[0][0]


def test_new_counts_by_state_uses_spec_partition_column(monkeypatch: pytest.MonkeyPatch) -> None:
    # The GROUP BY column is read from the spec (partition_column). Both partitioned serving tables
    # use "State" (DistrictVoter's mart `state` is renamed to "State" in the fresh cluster).
    voter_conn = FakeConn().queue_result([("TX", 1)])
    monkeypatch.setattr(step, "connect_new", fake_connect(voter_conn))
    step._new_counts_by_state(_CFG, "20260609", "Voter")
    assert 'GROUP BY "State"' in voter_conn.executed[0][0]

    dv_conn = FakeConn().queue_result([("TX", 1)])
    monkeypatch.setattr(step, "connect_new", fake_connect(dv_conn))
    step._new_counts_by_state(_CFG, "20260609", "DistrictVoter")
    assert 'GROUP BY "State"' in dv_conn.executed[0][0]


def test_new_total_count(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((42,))
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    assert step._new_total_count(_CFG, "20260609", "District") == 42


def test_new_total_count_queries_the_given_table(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((0,))
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    step._new_total_count(_CFG, "20260609", "DistrictStats")
    assert 'public."DistrictStats"' in conn.executed[0][0]


def test_new_total_count_no_row_returns_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()  # no queued result -> fetchone() returns None
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    assert step._new_total_count(_CFG, "20260609", "District") == 0


# --- _count_gate_check (partitioned vs flat branch) ---


def test_count_gate_check_partitioned_uses_per_state_compare(monkeypatch: pytest.MonkeyPatch) -> None:
    # DistrictVoter is partitioned (TABLE_SPECS) -> the per-state gate, not a flat total.
    monkeypatch.setattr(step, "_new_counts_by_state", lambda cfg, rd, table, **k: {"TX": 100})
    unload_table = _unload_table("DistrictVoter", {"TX": 100})
    check, actual = step._count_gate_check(_CFG, "20260609", unload_table)
    assert check.name == "row_counts_match_databricks:DistrictVoter"
    assert check.passed is True
    assert actual == {"TX": 100}
    assert "states" in check.details  # per-state detail shape from _compare_counts


def test_count_gate_check_flat_uses_total_compare(monkeypatch: pytest.MonkeyPatch) -> None:
    # District is flat (TABLE_SPECS) -> a single whole-table total, not per-state.
    monkeypatch.setattr(step, "_new_total_count", lambda cfg, rd, table, **k: 48)
    unload_table = _unload_table("District", {"": 50})
    check, actual = step._count_gate_check(_CFG, "20260609", unload_table)
    assert check.name == "row_counts_match_databricks:District"
    assert check.passed is True
    assert actual == 48
    assert "states" not in check.details  # flat gate compares a single total, not per-state


def test_count_gate_check_flat_outside_tolerance_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "_new_total_count", lambda cfg, rd, table, **k: 10)
    unload_table = _unload_table("District", {"": 50})
    check, _ = step._count_gate_check(_CFG, "20260609", unload_table)
    assert check.passed is False


def test_count_gate_check_voter_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "_new_counts_by_state", lambda cfg, rd, table, **k: {"TX": 100})
    unload_table = _unload_table("Voter", {"TX": 100})
    check, actual = step._count_gate_check(_CFG, "20260609", unload_table)
    assert check.name == "row_counts_match_databricks:Voter"
    assert check.passed is True
    assert actual == {"TX": 100}


def test_run_aggregates_and_writes_markdown(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    unload = _unload_all_tables()
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))
    monkeypatch.setattr(
        step, "put_artifact", lambda cfg, rd, sub, body: captured.setdefault("md", body) or "uri"
    )
    ok = step.ValidationCheck(name="x", passed=True, details={})
    bad = step.ValidationCheck(name="y", passed=False, details={})
    monkeypatch.setattr(step, "_new_counts_by_state", lambda *a, **k: {})
    monkeypatch.setattr(step, "_new_total_count", lambda *a, **k: 0)
    monkeypatch.setattr(step, "_compare_counts", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_compare_total", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_prod_row_counts", lambda *a, **k: [ok])
    monkeypatch.setattr(step, "_check_schema_diff", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_indexes", lambda *a, **k: bad)
    monkeypatch.setattr(step, "_check_sample_queries", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_l2type_coverage", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_indexes_valid", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_districtstats_buckets", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_index_usage", lambda *a, **k: ok)
    manifest = step.run(_CFG, "20260609")
    assert isinstance(manifest, ValidateManifest)
    assert manifest.all_passed is False
    # A failed gate must NOT write status="complete", or the skip-guard would
    # short-circuit any retry and return the cached failure.
    assert manifest.status == "failed"
    assert "Validation" in captured["md"]


def test_run_passes_writes_complete_status(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    unload = _unload_all_tables()
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: "uri")
    ok = step.ValidationCheck(name="x", passed=True, details={})
    monkeypatch.setattr(step, "_new_counts_by_state", lambda *a, **k: {})
    monkeypatch.setattr(step, "_new_total_count", lambda *a, **k: 0)
    monkeypatch.setattr(step, "_compare_counts", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_compare_total", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_prod_row_counts", lambda *a, **k: [ok])
    for name in (
        "_check_schema_diff",
        "_check_indexes",
        "_check_indexes_valid",
        "_check_districtstats_buckets",
        "_check_index_usage",
        "_check_sample_queries",
        "_check_l2type_coverage",
    ):
        monkeypatch.setattr(step, name, lambda *a, **k: ok)
    manifest = step.run(_CFG, "20260609")
    assert manifest.all_passed is True
    assert manifest.status == "complete"


def test_run_count_gate_runs_for_every_unload_table(monkeypatch: pytest.MonkeyPatch) -> None:
    # The unload-integrity gate must produce one row_counts_match_databricks check per table,
    # not just Voter — and a flat table (District/DistrictStats) must use the total branch while
    # a partitioned one (Voter/DistrictVoter) uses the per-state branch.
    unload = _unload_all_tables()
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: "uri")
    per_state_calls: list[str] = []
    total_calls: list[str] = []
    monkeypatch.setattr(
        step,
        "_new_counts_by_state",
        lambda cfg, rd, table, **k: (per_state_calls.append(table), {"TX": 100})[1],
    )
    monkeypatch.setattr(
        step, "_new_total_count", lambda cfg, rd, table, **k: (total_calls.append(table), 50)[1]
    )
    ok = step.ValidationCheck(name="x", passed=True, details={})
    monkeypatch.setattr(step, "_check_prod_row_counts", lambda *a, **k: [ok])
    monkeypatch.setattr(step, "_check_schema_diff", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_indexes", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_sample_queries", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_l2type_coverage", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_indexes_valid", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_districtstats_buckets", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_index_usage", lambda *a, **k: ok)
    manifest = step.run(_CFG, "20260609")
    names = {c.name for c in manifest.checks}
    assert "row_counts_match_databricks:Voter" in names
    assert "row_counts_match_databricks:District" in names
    assert "row_counts_match_databricks:DistrictStats" in names
    assert "row_counts_match_databricks:DistrictVoter" in names
    assert set(per_state_calls) == {"Voter", "DistrictVoter"}  # partitioned -> per-state branch
    assert set(total_calls) == {"District", "DistrictStats"}  # flat -> whole-table branch


def test_run_schema_and_index_checks_run_for_every_unload_table(monkeypatch: pytest.MonkeyPatch) -> None:
    unload = _unload_all_tables()
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: "uri")
    ok = step.ValidationCheck(name="x", passed=True, details={})
    monkeypatch.setattr(step, "_new_counts_by_state", lambda *a, **k: {})
    monkeypatch.setattr(step, "_new_total_count", lambda *a, **k: 0)
    monkeypatch.setattr(step, "_compare_counts", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_compare_total", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_prod_row_counts", lambda *a, **k: [ok])
    monkeypatch.setattr(step, "_check_sample_queries", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_l2type_coverage", lambda *a, **k: ok)
    schema_tables: list[str] = []
    index_tables: list[str] = []
    monkeypatch.setattr(
        step, "_check_schema_diff", lambda cfg, rd, table, **k: (schema_tables.append(table), ok)[1]
    )
    monkeypatch.setattr(
        step, "_check_indexes", lambda cfg, rd, table, **k: (index_tables.append(table), ok)[1]
    )
    monkeypatch.setattr(step, "_check_indexes_valid", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_districtstats_buckets", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_index_usage", lambda *a, **k: ok)
    step.run(_CFG, "20260609")
    assert schema_tables == ["Voter", "District", "DistrictStats", "DistrictVoter"]
    assert index_tables == ["Voter", "District", "DistrictStats", "DistrictVoter"]


def _boom(*args: object, **kwargs: object):
    raise RuntimeError("prod unreachable")


class _RaisingConn:
    """A connection whose cursor().execute always raises (for query-failure paths)."""

    def __enter__(self) -> _RaisingConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> _RaisingConn:
        return self

    def execute(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("query boom")

    def fetchone(self) -> object:
        return None


# --- _check_schema_diff ---


def test_check_schema_diff_clean_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([("col_a",), ("col_b",)])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("col_a",), ("col_b",)])))
    check = step._check_schema_diff(_CFG, "20260609", "Voter")
    assert check.name == "schema_diff_clean:Voter"
    assert check.passed is True
    assert check.details["missing_from_new"] == [] and check.details["extra_in_new"] == []


def test_check_schema_diff_missing_column_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([("col_a",), ("col_b",)])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("col_a",)])))
    check = step._check_schema_diff(_CFG, "20260609", "Voter")
    assert check.passed is False
    assert "col_b" in check.details["missing_from_new"]


def test_check_schema_diff_prod_unreachable_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", _boom)
    check = step._check_schema_diff(_CFG, "20260609", "Voter")
    assert check.passed is False and "error_reading_prod" in check.details


def test_check_schema_diff_partition_col_extra_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    # DistrictVoter is partitioned by "State"; the current serving copy lacks that column, so the
    # fresh cluster's "State" is the intended partitioning divergence, not drift -> pass.
    prod = [("district_id",), ("voter_id",), ("created_at",), ("updated_at",)]
    new = [*prod, ("State",)]
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result(prod)))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result(new)))
    check = step._check_schema_diff(_CFG, "20260609", "DistrictVoter")
    assert check.passed is True
    assert check.details["extra_in_new"] == ["State"]
    assert check.details["allowed_partition_extra"] == ["State"]


def test_check_schema_diff_non_partition_extra_still_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # Only the partition column is a free pass; any other extra column still fails.
    prod = [("district_id",), ("voter_id",), ("created_at",), ("updated_at",)]
    new = [*prod, ("State",), ("bogus",)]
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result(prod)))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result(new)))
    check = step._check_schema_diff(_CFG, "20260609", "DistrictVoter")
    assert check.passed is False
    assert "bogus" in check.details["extra_in_new"]


def test_check_schema_diff_different_table_uses_that_table(monkeypatch: pytest.MonkeyPatch) -> None:
    prod_conn = FakeConn().queue_result([("a",), ("b",)])
    monkeypatch.setattr(step, "connect_prod", fake_connect(prod_conn))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("a",), ("b",)])))
    check = step._check_schema_diff(_CFG, "20260609", "District")
    assert check.name == "schema_diff_clean:District"
    assert check.passed is True
    assert "table_name='District'" in prod_conn.executed[0][0]


def test_check_schema_diff_skips_table_absent_from_prod(monkeypatch: pytest.MonkeyPatch) -> None:
    # DistrictStats is "not yet a serving table" (schema_spec.py) — prod has no such table, so
    # information_schema returns zero columns. This must skip (passed=True), not report every
    # column on the new cluster as "extra" and fail forever.
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([])))
    check = step._check_schema_diff(_CFG, "20260609", "DistrictStats")
    assert check.name == "schema_diff_clean:DistrictStats"
    assert check.passed is True
    assert "skipped" in check.details


# --- _check_indexes ---


def test_check_indexes_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([("Voter_pkey",)])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Voter_pkey",)])))
    check = step._check_indexes(_CFG, "20260609", "Voter")
    assert check.name == "index_constraint_diff_clean:Voter"
    assert check.passed is True


def test_check_indexes_missing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        step, "connect_prod", fake_connect(FakeConn().queue_result([("Voter_pkey",), ("Voter_x_idx",)]))
    )
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Voter_pkey",)])))
    check = step._check_indexes(_CFG, "20260609", "Voter")
    assert check.passed is False
    assert "Voter_x_idx" in check.details["missing_from_new"]


def test_check_indexes_prod_unreachable_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", _boom)
    check = step._check_indexes(_CFG, "20260609", "Voter")
    assert check.passed is False and "error_reading_prod" in check.details


def test_check_indexes_different_table_uses_that_table(monkeypatch: pytest.MonkeyPatch) -> None:
    prod_conn = FakeConn().queue_result([("DistrictVoter_pkey",)])
    monkeypatch.setattr(step, "connect_prod", fake_connect(prod_conn))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("DistrictVoter_pkey",)])))
    check = step._check_indexes(_CFG, "20260609", "DistrictVoter")
    assert check.name == "index_constraint_diff_clean:DistrictVoter"
    assert check.passed is True
    assert "tablename='DistrictVoter'" in prod_conn.executed[0][0]


def test_check_indexes_table_absent_from_prod_passes_trivially(monkeypatch: pytest.MonkeyPatch) -> None:
    # DistrictStats has no prod indexes (the table doesn't exist yet on prod) -> nothing can be
    # "missing from new", so the gate passes without special-casing.
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("DistrictStats_pkey",)])))
    check = step._check_indexes(_CFG, "20260609", "DistrictStats")
    assert check.passed is True


# --- _check_indexes_valid (new-cluster index health) ---


def test_check_indexes_valid_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([])))
    check = step._check_indexes_valid(_CFG, "20260609", "Voter")
    assert check.name == "indexes_valid:Voter"
    assert check.passed is True
    assert check.details["invalid_indexes"] == []


def test_check_indexes_valid_fails_on_invalid(monkeypatch: pytest.MonkeyPatch) -> None:
    # A partition child that never attached leaves the parent index present-but-INVALID; the
    # name-only index_constraint_diff misses it, so this check must catch it.
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Voter_Age_Int_idx",)])))
    check = step._check_indexes_valid(_CFG, "20260609", "Voter")
    assert check.passed is False
    assert "Voter_Age_Int_idx" in check.details["invalid_indexes"]


# --- _check_districtstats_buckets (rename-shim sanity) ---

_GOOD_BUCKETS = {
    "age": [],
    "homeowner": [],
    "education": [],
    "presenceOfChildren": [],
    "estimatedIncomeRange": [],
}


def test_check_districtstats_buckets_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = (
        FakeConn()
        .queue_result(('public."DistrictStats"',))  # to_regclass
        .queue_result((0, 100))  # (null_buckets, total)
        .queue_result((_GOOD_BUCKETS,))  # sample buckets (psycopg3 decodes jsonb -> dict)
    )
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_districtstats_buckets(_CFG, "20260609")
    assert check.passed is True
    assert check.details["null_buckets"] == 0


def test_check_districtstats_buckets_missing_camelcase_keys_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    # snake_case leftover means the rename shim regressed -> camelCase keys missing -> fail.
    snake = {
        "age": [],
        "homeowner": [],
        "education": [],
        "presence_of_children": [],
        "estimated_income_range": [],
    }
    conn = FakeConn().queue_result(('public."DistrictStats"',)).queue_result((0, 100)).queue_result((snake,))
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_districtstats_buckets(_CFG, "20260609")
    assert check.passed is False
    assert "presenceOfChildren" in check.details["missing_keys"]


def test_check_districtstats_buckets_null_rows_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = (
        FakeConn()
        .queue_result(('public."DistrictStats"',))
        .queue_result((5, 100))  # 5 rows with NULL buckets
        .queue_result((_GOOD_BUCKETS,))
    )
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_districtstats_buckets(_CFG, "20260609")
    assert check.passed is False
    assert check.details["null_buckets"] == 5


def test_check_districtstats_buckets_absent_skips(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result((None,))))
    check = step._check_districtstats_buckets(_CFG, "20260609")
    assert check.passed is True
    assert check.details.get("skipped") == "DistrictStats absent"


# --- _check_index_usage (EXPLAIN: planner serves lookups via an index) ---


def test_check_index_usage_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    idx_plan = [("Append",), ('  ->  Index Scan using "Voter_pkey_TX" on "Voter_TX"',)]
    conn = FakeConn().queue_result(idx_plan).queue_result(idx_plan)  # one per lookup query
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_index_usage(_CFG, "20260609")
    assert check.name == "index_usage"
    assert check.passed is True
    assert set(check.details["pass"]) == {"id_lookup", "lalvoterid_lookup"}


def test_check_index_usage_seqscan_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    idx_plan = [("Append",), ('  ->  Index Scan using x on "Voter_TX"',)]
    seq_plan = [('Seq Scan on "Voter_TX"',)]  # a seq scan means the index wasn't planned
    conn = FakeConn().queue_result(idx_plan).queue_result(seq_plan)
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_index_usage(_CFG, "20260609")
    assert check.passed is False
    assert "lalvoterid_lookup" in check.details["fail"]


# --- _check_sample_queries (Voter-only) ---


def test_check_sample_queries_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    for _ in step._SAMPLE_QUERIES:
        conn.queue_result((1,))  # each query's fetchone returns a row
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_sample_queries(_CFG, "20260609")
    assert check.passed is True and not check.details["fail"]


def test_check_sample_queries_failure_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_new", lambda *a, **k: _RaisingConn())
    check = step._check_sample_queries(_CFG, "20260609")
    assert check.passed is False
    assert len(check.details["fail"]) == len(step._SAMPLE_QUERIES)


# --- _check_l2type_coverage (Voter-only) ---


def test_check_l2type_coverage_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([("Type_A",)])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Type_A",)])))
    assert step._check_l2type_coverage(_CFG, "20260609").passed is True


def test_check_l2type_coverage_missing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        step, "connect_prod", fake_connect(FakeConn().queue_result([("Type_A",), ("Type_B",)]))
    )
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Type_A",)])))
    check = step._check_l2type_coverage(_CFG, "20260609")
    assert check.passed is False
    assert "Type_B" in check.details["missing_columns"]


def test_check_l2type_coverage_skips_when_org_districts_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    # org_districts is an environmental dependency build_indexes already skips; validate
    # must skip too (passed=True, visibly recorded), not hard-fail and wedge the pipeline.
    monkeypatch.setattr(step, "connect_prod", _boom)
    check = step._check_l2type_coverage(_CFG, "20260609")
    assert check.passed is True
    assert "skipped" in check.details


def test_run_failed_manifest_reruns_checks(monkeypatch: pytest.MonkeyPatch) -> None:
    # A 'failed' validate manifest must NOT be skipped: the skip-guard short-circuits
    # only on 'complete', so a retry must re-execute every check and rewrite the manifest.
    # Pins the contract documented in run() (write 'failed', not 'complete', on a failure).
    captured: dict = {}
    unload = _unload_all_tables()
    failed = SimpleNamespace(status="failed")
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: failed if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: "uri")
    ok = step.ValidationCheck(name="x", passed=True, details={})
    monkeypatch.setattr(step, "_new_counts_by_state", lambda *a, **k: {})
    monkeypatch.setattr(step, "_new_total_count", lambda *a, **k: 0)
    monkeypatch.setattr(step, "_compare_counts", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_compare_total", lambda *a, **k: ok)
    monkeypatch.setattr(step, "_check_prod_row_counts", lambda *a, **k: [ok])
    for name in (
        "_check_schema_diff",
        "_check_indexes",
        "_check_indexes_valid",
        "_check_districtstats_buckets",
        "_check_index_usage",
        "_check_sample_queries",
        "_check_l2type_coverage",
    ):
        monkeypatch.setattr(step, name, lambda *a, **k: ok)
    manifest = step.run(_CFG, "20260609")
    assert "m" in captured, "checks must re-run (manifest written), not skip"
    assert manifest.status == "complete"


def test_run_writes_failed_manifest_when_new_cluster_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    # A naked pre-check failure must still leave a `failed` manifest (retryable), not
    # propagate out of run() with nothing written.
    captured: dict = {}
    unload = _unload_all_tables()
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))

    def _boom_counts(*a: object, **k: object) -> dict:
        raise RuntimeError("new cluster unreachable")

    monkeypatch.setattr(step, "_new_counts_by_state", _boom_counts)
    with pytest.raises(RuntimeError, match="new cluster unreachable"):
        step.run(_CFG, "20260609")
    assert captured["m"].status == "failed"
    assert captured["m"].all_passed is False


def test_run_requires_voter_in_unload_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    unload = SimpleNamespace(status="complete", tables=[SimpleNamespace(table="District", row_counts={})])
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    with pytest.raises(RuntimeError, match="no Voter table"):
        step.run(_CFG, "20260609")


# --- _check_prod_row_counts (inspect-prod baseline) ---


def test_check_prod_row_counts_fails_without_inspect_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    # Fail closed (not a silent pass): a passing skip would cache a `complete` manifest
    # that permanently bypasses this gate. Absent baseline -> failed (retryable).
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    checks = step._check_prod_row_counts(_CFG, "20260609", {"Voter": {"TX": 100}})
    assert len(checks) == 1
    assert checks[0].name == "prod_row_counts_within_tolerance:Voter"
    assert checks[0].passed is False
    assert "error" in checks[0].details


def test_check_prod_row_counts_fails_with_failed_inspect_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    # A non-complete (e.g. failed) inspect manifest must also fail closed — the guard is
    # `inspect is None or inspect.status != "complete"`, not just `inspect is None`.
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: SimpleNamespace(status="failed"))
    checks = step._check_prod_row_counts(_CFG, "20260609", {"Voter": {"TX": 100}})
    assert checks[0].passed is False
    assert "error" in checks[0].details


def test_check_prod_row_counts_compares_against_voter_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    inspect = SimpleNamespace(
        status="complete",
        tables=[SimpleNamespace(table="Voter", per_state_row_counts={"TX": 100}, total_row_count=100)],
    )
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: inspect)
    ok = step._check_prod_row_counts(_CFG, "20260609", {"Voter": {"TX": 105}})
    assert ok[0].name == "prod_row_counts_within_tolerance:Voter"
    assert ok[0].passed is True  # within ±10%
    bad = step._check_prod_row_counts(_CFG, "20260609", {"Voter": {"TX": 50}})
    assert bad[0].passed is False  # outside ±10%


def test_check_prod_row_counts_allows_new_state(monkeypatch: pytest.MonkeyPatch) -> None:
    # A state present in the new cluster but absent from the older prod snapshot is a
    # legitimate expansion, not drift — the prod gate must not flag it.
    inspect = SimpleNamespace(
        status="complete",
        tables=[SimpleNamespace(table="Voter", per_state_row_counts={"TX": 100}, total_row_count=100)],
    )
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: inspect)
    checks = step._check_prod_row_counts(_CFG, "20260609", {"Voter": {"TX": 100, "PR": 5}})
    assert checks[0].passed is True


def test_check_prod_row_counts_flat_table_compares_total(monkeypatch: pytest.MonkeyPatch) -> None:
    # District is flat -> compares the new total against inspect's total_row_count, not per-state.
    inspect = SimpleNamespace(
        status="complete",
        tables=[SimpleNamespace(table="District", per_state_row_counts={}, total_row_count=100)],
    )
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: inspect)
    checks = step._check_prod_row_counts(_CFG, "20260609", {"District": 105})
    assert checks[0].name == "prod_row_counts_within_tolerance:District"
    assert checks[0].passed is True
    fail = step._check_prod_row_counts(_CFG, "20260609", {"District": 10})
    assert fail[0].passed is False


def test_check_prod_row_counts_skips_table_not_in_prod_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    # DistrictStats is "not yet a serving table" — inspect_prod treats it as optional/best-effort
    # and omits it entirely when the prod cluster doesn't have it. There's no magnitude baseline
    # to sanity-check against: skip, don't fail closed (unlike the required Voter baseline).
    inspect = SimpleNamespace(
        status="complete",
        tables=[SimpleNamespace(table="Voter", per_state_row_counts={"TX": 100}, total_row_count=100)],
    )
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: inspect)
    checks = step._check_prod_row_counts(_CFG, "20260609", {"DistrictStats": 50})
    assert checks[0].name == "prod_row_counts_within_tolerance:DistrictStats"
    assert checks[0].passed is True
    assert "skipped" in checks[0].details


def test_check_prod_row_counts_voter_missing_from_manifest_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Voter is REQUIRED (inspect_prod guarantees it in any `complete` manifest); if it's somehow
    # absent, fail closed rather than silently skip like an optional District-family table.
    inspect = SimpleNamespace(status="complete", tables=[])
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: inspect)
    checks = step._check_prod_row_counts(_CFG, "20260609", {"Voter": {"TX": 100}})
    assert checks[0].passed is False
    assert "error" in checks[0].details


def test_check_prod_row_counts_voter_present_but_empty_baseline_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Voter's TableInspection exists but carries no per-state counts: the REQUIRED Voter baseline is
    # unusable, so this must fail closed (distinct from "absent entirely" for an optional table).
    inspect = SimpleNamespace(
        status="complete",
        tables=[SimpleNamespace(table="Voter", per_state_row_counts={}, total_row_count=0)],
    )
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: inspect)
    checks = step._check_prod_row_counts(_CFG, "20260609", {"Voter": {"TX": 100}})
    assert checks[0].passed is False
    assert "error" in checks[0].details


def test_check_prod_row_counts_non_voter_partitioned_empty_baseline_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # DistrictVoter is a partitioned serving table new to the loader; a current prod cluster may
    # carry it with no usable per-state baseline. Unlike Voter, this must SKIP (not fail closed) —
    # there's no magnitude baseline to assume, and failing would wedge the gate for a legit new table.
    inspect = SimpleNamespace(
        status="complete",
        tables=[
            SimpleNamespace(table="Voter", per_state_row_counts={"TX": 100}, total_row_count=100),
            SimpleNamespace(table="DistrictVoter", per_state_row_counts={}, total_row_count=0),
        ],
    )
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: inspect)
    checks = step._check_prod_row_counts(
        _CFG, "20260609", {"Voter": {"TX": 100}, "DistrictVoter": {"TX": 100}}
    )
    dv = next(c for c in checks if c.name == "prod_row_counts_within_tolerance:DistrictVoter")
    assert dv.passed is True
    assert "skipped" in dv.details
    # Voter with a real baseline still evaluated normally (fail-closed path untouched).
    voter = next(c for c in checks if c.name == "prod_row_counts_within_tolerance:Voter")
    assert voter.passed is True
