"""build-indexes: parses prod dump, builds PKs/indexes/FKs, ANALYZEs."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import build_indexes as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))

_DUMP = """
ALTER TABLE ONLY public."VoterTX" ADD CONSTRAINT "VoterTX_pkey" PRIMARY KEY ("LALVOTERID");
CREATE INDEX "VoterTX_LastName_idx" ON public."VoterTX" USING btree ("Voters_LastName");
"""


def test_run_builds_and_writes_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    conn = FakeConn()
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "connect_prod", fake_connect(FakeConn()))
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DUMP)
    monkeypatch.setattr(step, "_l2type_coverage", lambda cfg, rd, we: [])

    def _read(cfg, rd, name, model):
        return None  # indexes not done; unload absent (ordering falls back)

    monkeypatch.setattr(step, "read_manifest", _read)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")

    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    assert any("ADD CONSTRAINT" in s and "PRIMARY KEY" in s for s in sql)
    assert any("CREATE INDEX IF NOT EXISTS" in s for s in sql)
    assert any("ANALYZE" in s for s in sql)
    assert manifest.status == "complete"
    assert any(i.index_name == "VoterTX_LastName_idx" for i in manifest.indexes)
    assert "VoterTX_pkey" in manifest.constraints_added


def test_index_sql_rewrite_injects_if_not_exists() -> None:
    out = step._rewrite_index_sql('CREATE INDEX "x" ON public."VoterTX" (a);')
    assert "CREATE INDEX IF NOT EXISTS" in out
