"""build-indexes: applies PK + indexes (with State partition key) to public."Voter", then ANALYZE."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.steps import build_indexes as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))

# schema_spec records build_indexes reads (PK on id; a unique index + a plain index).
_PK = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id"])
_IDXS = [
    step.IndexDef(
        table="Voter", name="Voter_LALVOTERID_key", sql="", unique=True, columns=["LALVOTERID"], where=None
    ),
    step.IndexDef(
        table="Voter",
        name="Voter_Active_idx",
        sql='CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");',
        unique=False,
        columns=["Active"],
        where=None,
    ),
]


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
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "primary_key_for", lambda t: _PK)
    monkeypatch.setattr(step, "indexes_for", lambda t: _IDXS)
    monkeypatch.setattr(step, "_l2type_coverage", lambda cfg, rd: [])
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


def test_create_index_unique_preserves_where(monkeypatch: pytest.MonkeyPatch) -> None:
    # A partial unique index keeps its WHERE predicate (and gets State appended).
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    idx = step.IndexDef(
        table="Voter",
        name="Voter_u_idx",
        sql="(verbatim unused for unique)",
        unique=True,
        columns=["LALVOTERID"],
        where='"x" IS NOT NULL',
    )
    step._create_index(_CFG, "20260609", idx)
    sql = " ".join(executed_sql(conn))
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "Voter_u_idx"' in sql
    assert '("LALVOTERID", "State")' in sql
    assert 'WHERE "x" IS NOT NULL' in sql


def test_create_index_unique_functional_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    # A functional unique index can't be safely requoted from parsed columns with the
    # partition key — fail loudly instead of emitting invalid DDL.
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    idx = step.IndexDef(
        table="Voter",
        name="Voter_fn_uniq",
        sql="(unused)",
        unique=True,
        columns=['lower("Email")'],
        where=None,
    )
    with pytest.raises(RuntimeError, match="expression column"):
        step._create_index(_CFG, "20260609", idx)


def test_l2type_coverage_returns_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    # prod has Type_A + Type_B; new table only has a Type_A column -> Type_B missing.
    prod_conn = FakeConn().queue_result([("Type_A",), ("Type_B",)])
    new_conn = FakeConn().queue_result([("Type_A",)])
    monkeypatch.setattr(step, "connect_prod", fake_connect(prod_conn))
    monkeypatch.setattr(step, "connect_new", fake_connect(new_conn))
    assert step._l2type_coverage(_CFG, "20260609") == ["Type_B"]


def test_l2type_coverage_returns_none_when_prod_unreachable(monkeypatch: pytest.MonkeyPatch) -> None:
    # The None return (vs []) marks the check as skipped; it's load-bearing, so cover it.
    def _boom(*args: object, **kwargs: object) -> object:
        raise RuntimeError("org_districts unreachable")

    monkeypatch.setattr(step, "connect_prod", _boom)
    assert step._l2type_coverage(_CFG, "20260609") is None


class _PKRaisingConn:
    """Connection whose ALTER TABLE ... ADD CONSTRAINT raises a given psycopg error.

    Session SETs (non-ALTER) pass through, so only the PK statement triggers it.
    """

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def __enter__(self) -> _PKRaisingConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def cursor(self) -> _PKRaisingConn:
        return self

    def execute(self, sql: str, params: object = None) -> None:
        if sql.strip().startswith("ALTER TABLE"):
            raise self._exc


def test_add_primary_key_swallows_duplicate(monkeypatch: pytest.MonkeyPatch) -> None:
    # "constraint already exists" (DuplicateObject/42710) is the idempotency case.
    import psycopg

    monkeypatch.setattr(
        step, "connect_new", lambda *a, **k: _PKRaisingConn(psycopg.errors.DuplicateObject("exists"))
    )
    pk = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])
    step._add_primary_key(_CFG, "20260609", pk)  # must not raise


def test_add_primary_key_propagates_invalid_definition(monkeypatch: pytest.MonkeyPatch) -> None:
    # InvalidTableDefinition (42P16) is a structural rejection, not idempotency — it must
    # propagate so we never record an un-created PK as added.
    import psycopg

    monkeypatch.setattr(
        step,
        "connect_new",
        lambda *a, **k: _PKRaisingConn(psycopg.errors.InvalidTableDefinition("bad ddl")),
    )
    pk = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])
    with pytest.raises(psycopg.errors.InvalidTableDefinition):
        step._add_primary_key(_CFG, "20260609", pk)
