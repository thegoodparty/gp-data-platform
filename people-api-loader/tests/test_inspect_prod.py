"""inspect-prod: per-table prod counts + per-state snapshot dates -> InspectManifest."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import inspect_prod as step
from tests._fakes import FakeConn, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b", prod_cluster_id="gp-voter-db-x"))


def test_inspect_table_with_state_and_snapshot() -> None:
    # Voter-shaped: has State + updated_at -> per-state counts and per-state max(updated_at).
    dt = datetime(2026, 6, 1, 12, 0, 0)
    conn = (
        FakeConn()
        .queue_result((100,))  # count(*)
        .queue_result((1,))  # has "State"
        .queue_result([("TX", 60), ("CA", 40)])  # per-state counts
        .queue_result((1,))  # has "updated_at"
        .queue_result([("TX", dt), ("CA", dt)])  # per-state max(updated_at)
    )
    ti = step._inspect_table(conn.cursor(), "Voter")  # ty: ignore[invalid-argument-type]
    assert ti.total_row_count == 100
    assert ti.per_state_row_counts == {"TX": 60, "CA": 40}
    assert ti.per_state_snapshot_dates == {"TX": dt.isoformat(), "CA": dt.isoformat()}


def test_inspect_table_without_state_is_total_only() -> None:
    # A District table with no "State" column -> total count only, no per-state breakdown.
    conn = FakeConn().queue_result((7,)).queue_result(None)  # count(*); has "State" -> no
    ti = step._inspect_table(conn.cursor(), "District")  # ty: ignore[invalid-argument-type]
    assert ti.total_row_count == 7
    assert ti.per_state_row_counts == {}
    assert ti.per_state_snapshot_dates == {}


def test_run_writes_manifest_voter_first(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn()))
    seen: list[str] = []

    def _fake_inspect(cur: object, table: str) -> step.TableInspection:
        seen.append(table)
        return step.TableInspection(
            table=table, total_row_count=1, per_state_row_counts={"TX": 1} if table == "Voter" else {}
        )

    monkeypatch.setattr(step, "_inspect_table", _fake_inspect)
    manifest = step.run(_CFG, "20260609")
    assert manifest.status == "complete"
    assert seen[0] == "Voter"  # Voter inspected first (required)
    assert [t.table for t in manifest.tables] == ["Voter", "DistrictVoter", "District", "DistrictStats"]
    assert manifest.prod_cluster_id == "gp-voter-db-x"


def test_run_optional_table_failure_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    # A District table absent on this cluster is skipped, not fatal; Voter still required.
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: "uri")
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn()))

    def _fake_inspect(cur: object, table: str) -> step.TableInspection:
        if table == "DistrictStats":
            raise RuntimeError("relation does not exist")
        # Voter needs a non-empty per-state baseline or run() fails by design.
        return step.TableInspection(
            table=table, total_row_count=1, per_state_row_counts={"TX": 1} if table == "Voter" else {}
        )

    monkeypatch.setattr(step, "_inspect_table", _fake_inspect)
    manifest = step.run(_CFG, "20260609")
    assert manifest.status == "complete"
    assert {t.table for t in manifest.tables} == {"Voter", "DistrictVoter", "District"}


def test_run_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260609") is done


def test_run_raises_when_voter_has_no_per_state_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    # Empty Voter baseline must NOT write a "complete" manifest (would wedge the pipeline:
    # validate fails forever, inspect skip-guard blocks re-running). Fail before writing.
    wrote: dict = {}
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: wrote.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn()))
    monkeypatch.setattr(
        step,
        "_inspect_table",
        lambda cur, table: step.TableInspection(table=table, total_row_count=0, per_state_row_counts={}),
    )
    with pytest.raises(RuntimeError, match="no per-state row counts"):
        step.run(_CFG, "20260609")
    assert "m" not in wrote  # no manifest persisted
