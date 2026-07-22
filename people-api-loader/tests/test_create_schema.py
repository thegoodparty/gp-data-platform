"""create-schema: extracts CREATE TABLE for all four tables, installs extensions, applies DDL."""

from __future__ import annotations

from types import SimpleNamespace
from typing import cast

import pytest

from loader.people_api.config import LoaderConfig
from loader.people_api.schema.schema_spec import TABLE_SPECS
from loader.people_api.schema.states import STATES
from loader.people_api.steps import create_schema as step
from tests._fakes import FakeConn, executed_sql, fake_connect

_CFG = cast(LoaderConfig, SimpleNamespace(s3_bucket="b"))
_DUMP = (
    'CREATE TABLE public."Voter" ("id" uuid NOT NULL, "State" text NOT NULL);\n'
    'CREATE TABLE public."DistrictVoter" ("district_id" uuid NOT NULL, "voter_id" uuid NOT NULL, "State" text NOT NULL);\n'
    'CREATE TABLE public."District" ("id" uuid NOT NULL, "state" text NOT NULL);\n'
    'CREATE TABLE public."DistrictStats" ("district_id" uuid NOT NULL, "buckets" jsonb);\n'
)


def _patch(monkeypatch: pytest.MonkeyPatch, conn: FakeConn) -> dict:
    captured: dict = {}
    monkeypatch.setattr(step, "connect_new", fake_connect(conn))
    monkeypatch.setattr(step, "load_target_schema", lambda cfg, rd: _DUMP)
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
    assert any("CREATE EXTENSION IF NOT EXISTS pg_trgm" in s for s in sql)
    # parent table has PARTITION BY LIST and is retry-safe (IF NOT EXISTS)
    assert any('PARTITION BY LIST ("State")' in s for s in sql)
    assert any('CREATE TABLE IF NOT EXISTS public."Voter" (' in s for s in sql)
    # one child partition for TX
    assert any(
        'CREATE TABLE IF NOT EXISTS public."Voter_TX" PARTITION OF public."Voter" FOR VALUES IN (\'TX\')' in s
        for s in sql
    )
    # green compatibility views are created by run() over each public serving table
    assert any("CREATE SCHEMA IF NOT EXISTS green;" in s for s in sql)
    for t in ("Voter", "DistrictVoter", "District", "DistrictStats"):
        assert any(f'CREATE OR REPLACE VIEW green."{t}" AS SELECT * FROM public."{t}";' in s for s in sql)
    # indexes are NOT applied by create-schema
    assert not any("CREATE INDEX" in s for s in sql)
    assert manifest.status == "complete"
    assert "Voter" in manifest.tables_created
    assert "Voter_TX" in manifest.tables_created
    # Voter + its 51 state children + DistrictVoter + its 51 state children + District + DistrictStats
    assert len(manifest.tables_created) == 2 * (1 + len(STATES)) + 2


def test_creates_partitioned_and_flat(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    _patch(monkeypatch, conn)
    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    # Voter + DistrictVoter partitioned
    assert any(
        'CREATE TABLE IF NOT EXISTS public."Voter" (' in s and 'PARTITION BY LIST ("State")' in s for s in sql
    )
    # DistrictVoter partitions on the serving "State" (its mart `state` is renamed to "State").
    assert any(
        'CREATE TABLE IF NOT EXISTS public."DistrictVoter" (' in s and 'PARTITION BY LIST ("State")' in s
        for s in sql
    )
    assert any(
        'public."DistrictVoter_TX" PARTITION OF public."DistrictVoter" FOR VALUES IN (\'TX\')' in s
        for s in sql
    )
    # District + DistrictStats flat: plain create, NO partitioning
    assert any('CREATE TABLE IF NOT EXISTS public."District" (' in s for s in sql)
    assert any('CREATE TABLE IF NOT EXISTS public."DistrictStats" (' in s for s in sql)
    assert not any('public."District" ' in s and "PARTITION BY" in s for s in sql)
    # manifest lists every table + Voter/DistrictVoter children
    assert {"Voter", "DistrictVoter", "District", "DistrictStats"} <= set(manifest.tables_created)
    assert "Voter_TX" in manifest.tables_created and "DistrictVoter_TX" in manifest.tables_created


def test_partition_column_is_spec_driven_per_table(monkeypatch: pytest.MonkeyPatch) -> None:
    # The LIST-partition column is read from the spec (partition_column), not hardcoded. Both
    # partitioned tables use the serving "State" (DistrictVoter's mart `state` is renamed to "State"
    # via mart_column_map, so target_schema.sql carries "State" for it too).
    conn = FakeConn()
    _patch(monkeypatch, conn)
    step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    voter_parent = next(s for s in sql if 'CREATE TABLE IF NOT EXISTS public."Voter" (' in s)
    dv_parent = next(s for s in sql if 'CREATE TABLE IF NOT EXISTS public."DistrictVoter" (' in s)
    assert 'PARTITION BY LIST ("State")' in voter_parent
    assert 'PARTITION BY LIST ("State")' in dv_parent


def test_skips_when_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    done = SimpleNamespace(status="complete")
    monkeypatch.setattr(step, "read_manifest", lambda cfg, rd, name, model: done)
    monkeypatch.setattr(step, "manifest_uri", lambda cfg, rd, name: "s3://b/x")
    assert step.run(_CFG, "20260609") is done


def test_build_partitioned_ddl_shape() -> None:
    parent, children = step.build_partitioned_ddl(
        'CREATE TABLE public."Voter" ("id" uuid NOT NULL, "State" text NOT NULL);',
        "Voter",
        "State",
        ["TX", "CA"],
    )
    assert 'CREATE TABLE IF NOT EXISTS public."Voter"' in parent
    assert parent.rstrip().endswith('PARTITION BY LIST ("State");')
    assert children == [
        'CREATE TABLE IF NOT EXISTS public."Voter_TX" PARTITION OF public."Voter" FOR VALUES IN (\'TX\');',
        'CREATE TABLE IF NOT EXISTS public."Voter_CA" PARTITION OF public."Voter" FOR VALUES IN (\'CA\');',
    ]


def test_build_green_view_ddl_shape() -> None:
    assert step.build_green_view_ddl(["Voter", "District"]) == [
        "CREATE SCHEMA IF NOT EXISTS green;",
        'CREATE OR REPLACE VIEW green."Voter" AS SELECT * FROM public."Voter";',
        'CREATE OR REPLACE VIEW green."District" AS SELECT * FROM public."District";',
    ]


def test_build_green_view_ddl_covers_all_tables() -> None:
    stmts = step.build_green_view_ddl(list(TABLE_SPECS))
    assert len(stmts) == 1 + len(TABLE_SPECS)


# public."USState" labels, DC LAST, matching swain-db's public.USState order exactly (mirrors
# the private _USSTATE_LABELS in create_schema.py so a drift there is caught here too).
_USSTATE_LABELS = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
)


def test_build_usstate_enum_ddl_shape() -> None:
    ddl = step.build_usstate_enum_ddl()
    assert 'CREATE TYPE public."USState" AS ENUM' in ddl
    assert "EXCEPTION WHEN duplicate_object THEN NULL" in ddl
    assert len(_USSTATE_LABELS) == 51
    for label in _USSTATE_LABELS:
        assert f"'{label}'" in ddl
    assert ddl.count("'") == 2 * len(_USSTATE_LABELS)  # exactly 51 quoted labels, no duplicates
    # DC is last among the labels (matches swain-db's public.USState order).
    label_positions = [ddl.index(f"'{s}'") for s in _USSTATE_LABELS]
    assert label_positions == sorted(label_positions)
    assert _USSTATE_LABELS[-1] == "DC"


def test_usstate_type_created_before_voter_table(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    _patch(monkeypatch, conn)
    step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    enum_idx = next(i for i, s in enumerate(sql) if 'CREATE TYPE public."USState"' in s)
    voter_idx = next(i for i, s in enumerate(sql) if 'CREATE TABLE IF NOT EXISTS public."Voter" (' in s)
    assert enum_idx < voter_idx


def test_build_partitioned_ddl_parametrizes_table_and_column() -> None:
    parent, children = step.build_partitioned_ddl(
        'CREATE TABLE public."DistrictVoter" ("voter_id" uuid NOT NULL, "State" text NOT NULL);',
        "DistrictVoter",
        "State",
        ["TX"],
    )
    assert 'CREATE TABLE IF NOT EXISTS public."DistrictVoter"' in parent
    assert parent.rstrip().endswith('PARTITION BY LIST ("State");')
    assert children == [
        'CREATE TABLE IF NOT EXISTS public."DistrictVoter_TX" PARTITION OF public."DistrictVoter" FOR VALUES IN (\'TX\');'
    ]
