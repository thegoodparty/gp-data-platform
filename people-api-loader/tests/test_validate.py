"""validate: per-state GROUP BY counts at ±10%, five checks, markdown."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.manifests import ValidateManifest
from loader.people_api.steps import validate as step
from tests._fakes import FakeConn, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))


def test_row_counts_within_tolerance_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result([("TX", 105)])  # 105 vs expected 100 → within ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    assert step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100}).passed is True


def test_row_counts_outside_tolerance_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result([("TX", 50)])  # 50 vs 100 → outside ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100})
    assert check.passed is False
    assert check.details["mismatch_count"] == 1


def test_row_counts_expected_zero_requires_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([])))
    assert step._check_row_counts(_CFG, "20260609", "wh", {"TX": 0}).passed is True
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("TX", 1)])))
    assert step._check_row_counts(_CFG, "20260609", "wh", {"TX": 0}).passed is False


def test_row_counts_flags_unexpected_state(monkeypatch: pytest.MonkeyPatch) -> None:
    # A state present in the table but not in the baseline must not pass silently.
    conn = FakeConn().queue_result([("TX", 100), ("ZZ", 5)])
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100})
    assert check.passed is False
    assert "ZZ" in check.details["mismatches"]


def test_run_aggregates_and_writes_markdown(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    unload = SimpleNamespace(status="complete", per_state_row_counts={"TX": 100})
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(
        step, "put_artifact", lambda cfg, rd, sub, body: captured.setdefault("md", body) or "uri"
    )
    ok = step.ValidationCheck(name="x", passed=True, details={})
    bad = step.ValidationCheck(name="y", passed=False, details={})
    monkeypatch.setattr(step, "_check_row_counts", lambda *a: ok)
    monkeypatch.setattr(step, "_check_schema_diff", lambda *a: ok)
    monkeypatch.setattr(step, "_check_indexes", lambda *a: bad)
    monkeypatch.setattr(step, "_check_sample_queries", lambda *a: ok)
    monkeypatch.setattr(step, "_check_l2type_coverage", lambda *a: ok)
    manifest = step.run(_CFG, "20260609")
    assert isinstance(manifest, ValidateManifest)
    assert manifest.all_passed is False
    # A failed gate must NOT write status="complete", or the skip-guard would
    # short-circuit any retry and return the cached failure.
    assert manifest.status == "failed"
    assert "Validation" in captured["md"]


def test_run_passes_writes_complete_status(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    unload = SimpleNamespace(status="complete", per_state_row_counts={"TX": 100})
    monkeypatch.setattr(
        step, "read_manifest", lambda cfg, rd, name, model: None if name == "validate" else unload
    )
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: "uri")
    ok = step.ValidationCheck(name="x", passed=True, details={})
    for name in (
        "_check_row_counts",
        "_check_schema_diff",
        "_check_indexes",
        "_check_sample_queries",
        "_check_l2type_coverage",
    ):
        monkeypatch.setattr(step, name, lambda *a: ok)
    manifest = step.run(_CFG, "20260609")
    assert manifest.all_passed is True
    assert manifest.status == "complete"


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
    check = step._check_schema_diff(_CFG, "20260609", "wh")
    assert check.passed is True
    assert check.details["missing_from_new"] == [] and check.details["extra_in_new"] == []


def test_check_schema_diff_missing_column_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([("col_a",), ("col_b",)])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("col_a",)])))
    check = step._check_schema_diff(_CFG, "20260609", "wh")
    assert check.passed is False
    assert "col_b" in check.details["missing_from_new"]


def test_check_schema_diff_prod_unreachable_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", _boom)
    check = step._check_schema_diff(_CFG, "20260609", "wh")
    assert check.passed is False and "error_reading_prod" in check.details


# --- _check_indexes ---


def test_check_indexes_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([("Voter_pkey",)])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Voter_pkey",)])))
    assert step._check_indexes(_CFG, "20260609", "wh").passed is True


def test_check_indexes_missing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        step, "connect_prod", fake_connect(FakeConn().queue_result([("Voter_pkey",), ("Voter_x_idx",)]))
    )
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Voter_pkey",)])))
    check = step._check_indexes(_CFG, "20260609", "wh")
    assert check.passed is False
    assert "Voter_x_idx" in check.details["missing_from_new"]


def test_check_indexes_prod_unreachable_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", _boom)
    check = step._check_indexes(_CFG, "20260609", "wh")
    assert check.passed is False and "error_reading_prod" in check.details


# --- _check_sample_queries ---


def test_check_sample_queries_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    for _ in step._SAMPLE_QUERIES:
        conn.queue_result((1,))  # each query's fetchone returns a row
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_sample_queries(_CFG, "20260609", "wh")
    assert check.passed is True and not check.details["fail"]


def test_check_sample_queries_failure_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_new", lambda *a, **k: _RaisingConn())
    check = step._check_sample_queries(_CFG, "20260609", "wh")
    assert check.passed is False
    assert len(check.details["fail"]) == len(step._SAMPLE_QUERIES)


# --- _check_l2type_coverage ---


def test_check_l2type_coverage_pass(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn().queue_result([("Type_A",)])))
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Type_A",)])))
    assert step._check_l2type_coverage(_CFG, "20260609", "wh").passed is True


def test_check_l2type_coverage_missing_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        step, "connect_prod", fake_connect(FakeConn().queue_result([("Type_A",), ("Type_B",)]))
    )
    monkeypatch.setattr(step, "connect_new", fake_connect(FakeConn().queue_result([("Type_A",)])))
    check = step._check_l2type_coverage(_CFG, "20260609", "wh")
    assert check.passed is False
    assert "Type_B" in check.details["missing_columns"]


def test_check_l2type_coverage_prod_unreachable_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(step, "connect_prod", _boom)
    check = step._check_l2type_coverage(_CFG, "20260609", "wh")
    assert check.passed is False and "error_reading_org_districts" in check.details
