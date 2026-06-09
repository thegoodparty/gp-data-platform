"""validate: five checks, ±10% row-count tolerance, all_passed aggregation."""

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
    conn = FakeConn().queue_result((105,))  # 105 vs expected 100 → within ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100})
    assert check.passed is True


def test_row_counts_outside_tolerance_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn().queue_result((50,))  # 50 vs 100 → outside ±10%
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    check = step._check_row_counts(_CFG, "20260609", "wh", {"TX": 100})
    assert check.passed is False
    assert check.details["mismatch_count"] == 1


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
    # Stub the individual checks to avoid DB; assert orchestration + all_passed.
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
    assert "Validation" in captured["md"]
