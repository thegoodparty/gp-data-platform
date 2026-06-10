"""build-indexes: applies PK + indexes (with State partition key) to public."Voter", then ANALYZE."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import build_indexes as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))

_DUMP = """
ALTER TABLE ONLY public."Voter" ADD CONSTRAINT "Voter_pkey" PRIMARY KEY (id);
CREATE UNIQUE INDEX "Voter_LALVOTERID_key" ON public."Voter" USING btree ("LALVOTERID");
CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");
"""


def test_rewrite_injects_if_not_exists() -> None:
    assert "CREATE INDEX IF NOT EXISTS" in step._rewrite_index_sql(
        'CREATE INDEX "x" ON public."Voter" ("Active");'
    )
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in step._rewrite_index_sql(
        'CREATE UNIQUE INDEX "u" ON public."Voter" ("LALVOTERID");'
    )


def test_run_builds_pk_indexes_and_analyzes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    conn = FakeConn()
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DUMP)
    monkeypatch.setattr(step, "_l2type_coverage", lambda cfg, rd, we: [])
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")

    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)

    # PK must include "State"
    assert any("ADD CONSTRAINT" in s and 'PRIMARY KEY ("id", "State")' in s for s in sql)
    # Unique index must include "State"
    assert any(
        'CREATE UNIQUE INDEX IF NOT EXISTS "Voter_LALVOTERID_key" ON public."Voter" ("LALVOTERID", "State")'
        in s
        for s in sql
    )
    # Plain index should be rewritten with IF NOT EXISTS but NOT have "State" appended
    assert any("CREATE INDEX IF NOT EXISTS" in s and "Voter_Active_idx" in s for s in sql)
    assert any('ANALYZE public."Voter"' in s for s in sql)
    assert manifest.status == "complete"
    assert manifest.analyzed_tables == ["Voter"]
    assert "Voter_pkey" in manifest.constraints_added
    assert {i.index_name for i in manifest.indexes} == {"Voter_LALVOTERID_key", "Voter_Active_idx"}
