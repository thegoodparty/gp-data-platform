"""analyze: a bare database-wide ANALYZE (every leaf partition) as the final step + manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import analyze as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(new_cluster_id=lambda rd: f"gp-people-db-{rd}"))


def test_analyze_runs_bare_analyze_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    conn = FakeConn().queue_result((104,))  # the analyzed-relation count query
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))

    manifest = step.run(_CFG, "20260728")

    assert manifest.status == "complete"
    assert manifest.tables_analyzed == 104
    sqls = executed_sql(conn)
    # A bare, whole-database ANALYZE — it reaches every leaf partition, unlike a per-parent
    # `ANALYZE public."Voter"`, which only refreshes the parent's inheritance stats.
    assert sqls[0] == "ANALYZE"
    assert not any('ANALYZE public."' in s for s in sqls)


def test_analyze_skips_completed_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "uri")
    assert step.run(_CFG, "20260728") is done
