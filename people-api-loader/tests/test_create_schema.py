"""create-schema: extracts CREATE TABLE Voter, installs extensions, applies partitioned DDL."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import create_schema as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))
_DUMP = (
    'CREATE TABLE public."Voter" ("id" uuid NOT NULL, "State" text NOT NULL);\n'
    'CREATE INDEX "Voter_State_idx" ON public."Voter" USING btree ("State");\n'
)


def _patch(monkeypatch: pytest.MonkeyPatch, conn: FakeConn) -> dict:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "load_prod_dump", lambda cfg, rd: _DUMP)
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: f"s3://b/{sub}")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    return captured


def test_applies_partitioned_table_and_extensions(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    _patch(monkeypatch, conn)
    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE" in s for s in sql)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_commons" in s for s in sql)
    # parent table has PARTITION BY LIST
    assert any('PARTITION BY LIST ("State")' in s for s in sql)
    # one child partition for TX
    assert any(
        'CREATE TABLE IF NOT EXISTS public."Voter_TX" PARTITION OF public."Voter" FOR VALUES IN (\'TX\')' in s
        for s in sql
    )
    # indexes are NOT applied by create-schema
    assert not any("CREATE INDEX" in s for s in sql)
    assert manifest.status == "complete"
    assert "Voter" in manifest.tables_created
    assert "Voter_TX" in manifest.tables_created
    assert len(manifest.tables_created) == 52


def test_skips_when_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "s3://b/x")
    assert step.run(_CFG, "20260609") is done
