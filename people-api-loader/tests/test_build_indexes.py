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


def test_child_index_name_short_and_hashed() -> None:
    assert step._child_index_name("Voter_Active_idx", "CA") == "Voter_Active_idx_CA"
    # A very long parent name + state would blow the 63-char identifier limit -> hashed fallback.
    long = "Voter_" + "Really_Long_District_Name_" * 3 + "idx"
    child = step._child_index_name(long, "WY")
    assert len(child) <= 63 and child.startswith("ix_") and child.endswith("_WY")


def test_plain_parent_only_and_child_sql() -> None:
    idx = step.IndexDef(
        table="Voter",
        name="Voter_Active_idx",
        sql='CREATE INDEX "Voter_Active_idx" ON public."Voter" USING btree ("Active");',
        unique=False,
        columns=["Active"],
        where=None,
    )
    parent = step._plain_parent_only_sql(idx)
    assert (
        'CREATE INDEX IF NOT EXISTS "Voter_Active_idx" ON ONLY public."Voter" USING btree ("Active")'
        in parent
    )
    child_name, child_sql = step._plain_child_sql(idx, "CA")
    assert child_name == "Voter_Active_idx_CA"
    assert (
        'CREATE INDEX IF NOT EXISTS "Voter_Active_idx_CA" ON public."Voter_CA" USING btree ("Active")'
        in child_sql
    )
    assert "ON ONLY" not in child_sql  # the child targets the leaf partition, not the parent


def test_build_and_attach_child_skips_reattach_when_already_attached() -> None:
    # pg_inherits returns a row => the child is already a partition of the parent index; a
    # partial-rerun must build (IF NOT EXISTS, no-op) but NOT re-issue ATTACH (which would error).
    conn = FakeConn().queue_result((1,))
    step._build_and_attach_child(conn, (_IDXS[1], "CA"))  # ty: ignore[invalid-argument-type]
    sql = executed_sql(conn)
    assert any('"Voter_Active_idx_CA" ON public."Voter_CA"' in s for s in sql)
    assert not any("ATTACH PARTITION" in s for s in sql)


def test_run_builds_pk_indexes_and_analyzes(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}
    conn = FakeConn()
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "open_new_tunnel", fake_connect(None))  # no bastion in unit tests
    monkeypatch.setattr(step, "primary_key_for", lambda t: _PK)
    monkeypatch.setattr(step, "indexes_for", lambda t: _IDXS)
    monkeypatch.setattr(step, "_l2type_coverage", lambda cfg, rd, **_k: [])
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: None)
    monkeypatch.setattr(step, "write_manifest", lambda cfg, m: captured.setdefault("m", m) or "uri")
    monkeypatch.setattr(step, "STATES", ("CA", "TX"))

    # parallelism=1 keeps the persistent-connection pool single-threaded, so all stages run on the
    # one shared FakeConn deterministically (no cross-thread races on its recorded-SQL list).
    manifest = step.run(_CFG, "20260609", parallelism=1)
    sql = executed_sql(conn)

    # PK must include "State"
    assert any("ADD CONSTRAINT" in s and 'PRIMARY KEY ("id", "State")' in s for s in sql)
    # Unique index stays a parent-level build, with "State" appended
    assert any(
        'CREATE UNIQUE INDEX IF NOT EXISTS "Voter_LALVOTERID_key" ON public."Voter" ("LALVOTERID", "State")'
        in s
        for s in sql
    )
    # Plain index is built PER PARTITION: empty parent (ON ONLY) + a child per state + ATTACH.
    assert any('CREATE INDEX IF NOT EXISTS "Voter_Active_idx" ON ONLY public."Voter"' in s for s in sql)
    assert any('"Voter_Active_idx_CA" ON public."Voter_CA"' in s for s in sql)
    assert any('"Voter_Active_idx_TX" ON public."Voter_TX"' in s for s in sql)
    assert any(
        'ALTER INDEX public."Voter_Active_idx" ATTACH PARTITION public."Voter_Active_idx_CA"' in s
        for s in sql
    )
    # ...and NOT built directly on the parent (that's the slow serial-per-partition path we removed).
    assert not any('"Voter_Active_idx" ON public."Voter" USING' in s for s in sql)
    assert any('ANALYZE public."Voter"' in s for s in sql)
    assert manifest.status == "complete"
    assert manifest.analyzed_tables == ["Voter"]
    assert "Voter_pkey" in manifest.constraints_added
    assert {i.index_name for i in manifest.indexes} == {"Voter_LALVOTERID_key", "Voter_Active_idx"}


def test_create_index_unique_preserves_where(monkeypatch: pytest.MonkeyPatch) -> None:
    # A partial unique index keeps its WHERE predicate (and gets State appended).
    conn = FakeConn()
    idx = step.IndexDef(
        table="Voter",
        name="Voter_u_idx",
        sql="(verbatim unused for unique)",
        unique=True,
        columns=["LALVOTERID"],
        where='"x" IS NOT NULL',
    )
    step._create_index(conn, idx)  # ty: ignore[invalid-argument-type]
    sql = " ".join(executed_sql(conn))
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "Voter_u_idx"' in sql
    assert '("LALVOTERID", "State")' in sql
    assert 'WHERE "x" IS NOT NULL' in sql


def test_create_index_unique_functional_raises() -> None:
    # A functional unique index can't be safely requoted from parsed columns with the
    # partition key — fail loudly instead of emitting invalid DDL.
    conn = FakeConn()
    idx = step.IndexDef(
        table="Voter",
        name="Voter_fn_uniq",
        sql="(unused)",
        unique=True,
        columns=['lower("Email")'],
        where=None,
    )
    with pytest.raises(RuntimeError, match="expression column"):
        step._create_index(conn, idx)  # ty: ignore[invalid-argument-type]


def test_create_index_unique_empty_columns_raises() -> None:
    # A unique index with no parsed columns must NOT silently rebuild to UNIQUE("State") —
    # guards the extraction regression the seed once had (columns=[]).
    conn = FakeConn()
    idx = step.IndexDef(
        table="Voter", name="Voter_LALVOTERID_key", sql="(unused)", unique=True, columns=[], where=None
    )
    with pytest.raises(RuntimeError, match="no parsed columns"):
        step._create_index(conn, idx)  # ty: ignore[invalid-argument-type]


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

    def fetchone(self) -> object:
        # The idempotency pre-check ("does a PK already exist?") sees none, so _add_primary_key
        # proceeds to the ADD — where ALTER TABLE raises the injected error.
        return None

    def execute(self, sql: str, params: object = None) -> None:
        if sql.strip().startswith("ALTER TABLE"):
            raise self._exc


def test_add_primary_key_skips_when_pk_exists() -> None:
    # Re-runnability: a table can hold only one PK, so a re-run on a cluster that already has it
    # must SKIP the ADD (which would raise 42P16), not re-issue it. The pre-check finds the PK.
    conn = FakeConn().queue_result(("Voter_pkey",))
    pk = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])
    step._add_primary_key(conn, pk)  # ty: ignore[invalid-argument-type]  # must not raise
    assert not any("ADD CONSTRAINT" in s for s in executed_sql(conn))


def test_add_primary_key_propagates_invalid_definition() -> None:
    # With no existing PK, a genuine bad-DDL InvalidTableDefinition (42P16) on the ADD is a
    # structural rejection, not idempotency — it must propagate, never recorded as added.
    import psycopg

    pk = step.PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])
    with pytest.raises(psycopg.errors.InvalidTableDefinition):
        step._add_primary_key(
            _PKRaisingConn(psycopg.errors.InvalidTableDefinition("bad ddl")), pk
        )  # ty: ignore[invalid-argument-type]
