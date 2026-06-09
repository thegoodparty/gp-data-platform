"""create-schema: emits DDL, installs extensions, applies it, writes manifest."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import create_schema as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))


def _patch_common(monkeypatch: pytest.MonkeyPatch, conn: FakeConn, unload=None) -> dict:
    captured: dict = {}
    monkeypatch.setattr(step, "resolve_writer_endpoint", lambda cfg, rd: "wh")
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(
        step, "load_prod_dump", lambda cfg, rd: 'CREATE TABLE public."VoterTX" ("LALVOTERID" text);'
    )
    monkeypatch.setattr(step, "load_databricks_columns", lambda cfg, rd: {})
    monkeypatch.setattr(step, "put_artifact", lambda cfg, rd, sub, body: f"s3://b/{sub}")

    # read_manifest: schema -> None (not done); unload -> provided
    def _read(cfg, rd, name, model):
        return None if name == "schema" else unload

    monkeypatch.setattr(step, "read_manifest", _read)
    monkeypatch.setattr(
        step,
        "write_manifest",
        lambda cfg, m: captured.setdefault("manifest", m) or "s3://b/_manifest/schema.json",
    )
    return captured


def test_applies_extensions_and_ddl(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    _patch_common(monkeypatch, conn)
    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE" in s for s in sql)
    assert any("CREATE EXTENSION IF NOT EXISTS aws_commons" in s for s in sql)
    assert any('CREATE TABLE public."VoterTX"' in s for s in sql)
    assert manifest.status == "complete"
    assert "VoterFile" in manifest.tables_created
    assert len(manifest.tables_created) == 52  # 51 states + VoterFile


def test_skips_when_manifest_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "s3://b/x")
    assert step.run(_CFG, "20260609") is done
