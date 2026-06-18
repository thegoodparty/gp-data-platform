# People-API DDL Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Generate the loader's target Postgres schema from the people-api dbt marts (columns/types) plus a version-controlled serving-structure spec (PK/indexes/partitions seeded from prod), retiring the committed `prod_dump.sql`.

**Architecture:** A `loader emit-ddl` CLI introspects the four Databricks people_api marts via the Unity Catalog metadata API, maps Spark types to Postgres, and renders a committed `target_schema.sql` (CREATE TABLE blocks in pg_dump shape, so the existing `extract_create_tables` parser reads them). PK/indexes/FKs come from `schema_spec.py`, whose index data is generated into a committed `_serving_seed.py` by a one-time `extract-serving-structure` step that reads `pg_catalog` on the live prod cluster. `create_schema` and `build_indexes` then read these generated artifacts instead of `prod_dump.sql`.

**Tech Stack:** Python 3.14, uv, ruff (line-length 110), ty (pinned), pytest, Typer CLI, psycopg 3, `databricks-sdk` (Unity Catalog `tables.get`), pydantic manifests.

**Branch:** `feat/DATA-1909-cluster-lifecycle` (PR #504). Run `uv run pytest -q`, `uv run ruff check . && uv run ruff format .`, `uv run ty check` after each task; run `pre-commit run --all-files` from the repo root before any push.

**Spec:** `people-api-loader/docs/superpowers/specs/2026-06-18-people-api-ddl-generation-design.md`

All paths below are relative to `people-api-loader/`.

---

## File structure

**New:**
- `src/loader/people_api/schema/type_map.py` — Spark/Delta type string → Postgres type.
- `src/loader/people_api/schema/mart_introspect.py` — read mart columns from Databricks UC.
- `src/loader/people_api/schema/serving_structure.py` — pg_catalog → PK/index/FK records.
- `src/loader/people_api/schema/_serving_seed.py` — GENERATED committed data (per-table PK/index/FK records); produced by `extract-serving-structure`.
- `src/loader/people_api/schema/schema_spec.py` — declarative per-table spec composing marts + `_serving_seed`.
- `src/loader/people_api/schema/emit_ddl.py` — renderer: marts + spec → `target_schema.sql`.
- `tests/test_type_map.py`, `tests/test_mart_introspect.py`, `tests/test_serving_structure.py`, `tests/test_schema_spec.py`, `tests/test_emit_ddl.py`.

**Modified:**
- `src/loader/people_api/config.py` — mart FQNs config.
- `src/loader/people_api/cli.py` — `emit-ddl` + `extract-serving-structure` commands.
- `src/loader/people_api/steps/inspect_prod.py` — also write the serving-structure artifact.
- `src/loader/people_api/steps/create_schema.py` — read `target_schema.sql`, create all four tables.
- `src/loader/people_api/steps/build_indexes.py` — read PK/index/FK from `schema_spec`, all four tables.
- `tests/test_create_schema.py`, `tests/test_build_indexes.py` — updated for the new source.

**Removed (Phase 2):**
- `src/loader/people_api/schema/data/prod_dump.sql`, `src/loader/people_api/schema/snapshot.py`, the parser functions in `index_specs.py` (the dataclasses stay).

---

# PHASE 1 — generate and validate in parallel (no behavior change)

## Task 1: Spark→Postgres type map

**Files:**
- Create: `src/loader/people_api/schema/type_map.py`
- Test: `tests/test_type_map.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_type_map.py
"""Spark/Delta type strings -> Postgres types."""

from __future__ import annotations

import pytest

from loader.people_api.schema.type_map import UnknownSparkType, to_pg_type


@pytest.mark.parametrize(
    ("spark", "pg"),
    [
        ("string", "TEXT"),
        ("int", "INTEGER"),
        ("integer", "INTEGER"),
        ("bigint", "BIGINT"),
        ("smallint", "SMALLINT"),
        ("double", "DOUBLE PRECISION"),
        ("float", "REAL"),
        ("boolean", "BOOLEAN"),
        ("date", "DATE"),
        ("timestamp", "TIMESTAMPTZ"),
        ("timestamp_ntz", "TIMESTAMP"),
        ("binary", "BYTEA"),
        ("decimal(10,2)", "NUMERIC(10,2)"),
        ("decimal(38, 0)", "NUMERIC(38,0)"),
        ("DECIMAL(5,4)", "NUMERIC(5,4)"),  # case-insensitive
        ("  string  ", "TEXT"),  # whitespace tolerant
    ],
)
def test_known_types(spark: str, pg: str) -> None:
    assert to_pg_type(spark) == pg


def test_unknown_type_raises() -> None:
    with pytest.raises(UnknownSparkType, match="struct"):
        to_pg_type("struct<a:int>")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_type_map.py -q`
Expected: FAIL with `ModuleNotFoundError: loader.people_api.schema.type_map`

- [ ] **Step 3: Write the implementation**

```python
# src/loader/people_api/schema/type_map.py
"""Map Databricks/Spark type strings (as returned by Unity Catalog `type_text`)
to Postgres column types. Fail loud on anything unrecognised — never guess a type."""

from __future__ import annotations

import re

_DECIMAL_RE = re.compile(r"^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$")

_SCALAR: dict[str, str] = {
    "string": "TEXT",
    "int": "INTEGER",
    "integer": "INTEGER",
    "bigint": "BIGINT",
    "long": "BIGINT",
    "smallint": "SMALLINT",
    "short": "SMALLINT",
    "tinyint": "SMALLINT",
    "byte": "SMALLINT",
    "double": "DOUBLE PRECISION",
    "float": "REAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "timestamp": "TIMESTAMPTZ",
    "timestamp_ntz": "TIMESTAMP",
    "binary": "BYTEA",
}


class UnknownSparkType(RuntimeError):
    """Raised for a Spark type with no Postgres mapping (so we never emit a wrong type)."""


def to_pg_type(spark_type: str) -> str:
    t = spark_type.strip().lower()
    if t in _SCALAR:
        return _SCALAR[t]
    m = _DECIMAL_RE.match(t)
    if m:
        return f"NUMERIC({m.group(1)},{m.group(2)})"
    raise UnknownSparkType(
        f"no Postgres mapping for Spark type {spark_type!r}; add it to type_map._SCALAR "
        "or handle it as a per-column override in schema_spec"
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_type_map.py -q`
Expected: PASS (17 cases).

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/schema/type_map.py tests/test_type_map.py
git commit -m "feat(loader): Spark->Postgres type map for DDL generation"
```

---

## Task 2: Mart column introspection (Databricks UC)

**Files:**
- Create: `src/loader/people_api/schema/mart_introspect.py`
- Test: `tests/test_mart_introspect.py`

Uses the Unity Catalog metadata API (`WorkspaceClient().tables.get(fqn).columns`) — no SQL warehouse. Each column exposes `.name`, `.type_text`, `.nullable`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_mart_introspect.py
"""Mart introspection turns UC table columns into ordered (name, spark_type, nullable)."""

from __future__ import annotations

from types import SimpleNamespace

from loader.people_api.schema.mart_introspect import MartColumn, columns_from_uc_table


def test_columns_from_uc_table_preserves_order_and_fields() -> None:
    fake_table = SimpleNamespace(
        columns=[
            SimpleNamespace(name="id", type_text="string", nullable=False, position=0),
            SimpleNamespace(name="Age_Int", type_text="int", nullable=True, position=1),
        ]
    )
    assert columns_from_uc_table(fake_table) == [
        MartColumn(name="id", spark_type="string", nullable=False),
        MartColumn(name="Age_Int", spark_type="int", nullable=True),
    ]


def test_columns_sorted_by_position_when_unordered() -> None:
    fake_table = SimpleNamespace(
        columns=[
            SimpleNamespace(name="b", type_text="int", nullable=True, position=1),
            SimpleNamespace(name="a", type_text="int", nullable=True, position=0),
        ]
    )
    assert [c.name for c in columns_from_uc_table(fake_table)] == ["a", "b"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_mart_introspect.py -q`
Expected: FAIL with `ModuleNotFoundError`.

- [ ] **Step 3: Write the implementation**

```python
# src/loader/people_api/schema/mart_introspect.py
"""Read a dbt mart's column schema from Databricks Unity Catalog.

Uses the UC metadata API (no SQL warehouse): WorkspaceClient().tables.get(fqn).
WorkspaceClient() picks up auth from the standard databricks env/config
(DATABRICKS_HOST + token, or a profile)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class MartColumn:
    name: str
    spark_type: str  # UC type_text, e.g. "string", "int", "decimal(10,2)"
    nullable: bool


def columns_from_uc_table(table: Any) -> list[MartColumn]:
    """Map a UC TableInfo to ordered MartColumns (sorted by column position)."""
    cols = sorted(table.columns, key=lambda c: c.position)
    return [MartColumn(name=c.name, spark_type=c.type_text, nullable=bool(c.nullable)) for c in cols]


def introspect_mart(fqn: str) -> list[MartColumn]:
    """Fetch `<catalog>.<schema>.<table>` column schema from Unity Catalog.

    Imported lazily so unit tests (which pass a fake table to columns_from_uc_table)
    don't require the databricks SDK or live credentials.
    """
    from databricks.sdk import WorkspaceClient

    table = WorkspaceClient().tables.get(full_name=fqn)
    if not getattr(table, "columns", None):
        raise RuntimeError(f"Unity Catalog table {fqn!r} returned no columns")
    return columns_from_uc_table(table)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_mart_introspect.py -q`
Expected: PASS (2 cases).

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/schema/mart_introspect.py tests/test_mart_introspect.py
git commit -m "feat(loader): introspect dbt mart columns from Unity Catalog"
```

---

## Task 3: Mart FQN config

**Files:**
- Modify: `src/loader/people_api/config.py`
- Test: `tests/test_config.py` (add a case; create the file if absent)

Add the four mart fully-qualified names, defaulting to the `goodparty_data_catalog.dbt` schema the int model already uses, overridable via env. PG table name ↔ mart FQN mapping lives here so `schema_spec` and `emit_ddl` share one source.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_config.py  (add this test; keep existing ones if the file exists)
def test_people_api_mart_fqns_default(monkeypatch) -> None:
    import os

    from loader.people_api.config import LoaderConfig

    for k in list(os.environ):
        if k.startswith("LOADER_MART_"):
            monkeypatch.delenv(k, raising=False)
    cfg = LoaderConfig.from_env()
    assert cfg.mart_fqns["Voter"] == "goodparty_data_catalog.dbt.m_people_api__voter"
    assert set(cfg.mart_fqns) == {"Voter", "District", "DistrictStats", "DistrictVoter"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_config.py::test_people_api_mart_fqns_default -q`
Expected: FAIL (`AttributeError: ... 'mart_fqns'`).

- [ ] **Step 3: Implement (config.py)**

Add near the other defaults:

```python
# DDL generation reads column schemas from these dbt marts (Unity Catalog). The PG table
# name (key) maps to the mart's fully-qualified name. Default schema matches the int model.
DEFAULT_MART_CATALOG_SCHEMA = "goodparty_data_catalog.dbt"
_MART_MODELS = {
    "Voter": "m_people_api__voter",
    "District": "m_people_api__district",
    "DistrictStats": "m_people_api__districtstats",
    "DistrictVoter": "m_people_api__districtvoter",
}
```

Add a field to `LoaderConfig` (with the other fields):

```python
    # PG table name -> Databricks mart FQN (column/type source for emit-ddl).
    mart_fqns: dict[str, str]
```

In `from_env()`, build it (override the catalog.schema via `LOADER_MART_CATALOG_SCHEMA`):

```python
        mart_schema = os.environ.get("LOADER_MART_CATALOG_SCHEMA", DEFAULT_MART_CATALOG_SCHEMA)
        mart_fqns = {pg: f"{mart_schema}.{model}" for pg, model in _MART_MODELS.items()}
```

and pass `mart_fqns=mart_fqns` to the `cls(...)` call.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_config.py::test_people_api_mart_fqns_default -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/config.py tests/test_config.py
git commit -m "feat(loader): config for people_api mart FQNs"
```

---

## Task 4: Serving-structure extraction from pg_catalog

**Files:**
- Create: `src/loader/people_api/schema/serving_structure.py`
- Test: `tests/test_serving_structure.py`

Reuses the `PrimaryKey`/`IndexDef`/`ForeignKey` dataclasses from `schema/index_specs.py`. Queries `pg_catalog` (via a psycopg cursor) and returns records per table. The functions take a cursor so they're unit-testable with a fake.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_serving_structure.py
"""Serving-structure extraction maps pg_catalog rows to index/PK/FK dataclasses."""

from __future__ import annotations

from loader.people_api.schema.serving_structure import (
    extract_foreign_keys,
    extract_indexes,
    extract_primary_keys,
)


class _FakeCursor:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def execute(self, sql: str, params: object = None) -> None:
        self._executed = (sql, params)

    def fetchall(self) -> list[tuple]:
        return self._rows


def test_extract_indexes_skips_pk_and_keeps_definition() -> None:
    cur = _FakeCursor(
        [
            ("Voter", "Voter_LastName_idx", 'CREATE INDEX "Voter_LastName_idx" ON public."Voter" '
             'USING btree ("LastName")', False),
            ("Voter", "Voter_family_idx", 'CREATE INDEX "Voter_family_idx" ON public."Voter" '
             'USING btree ("Mailing_Families_FamilyID") WHERE ("Mailing_Families_FamilyID" IS NOT NULL)', False),
        ]
    )
    idxs = extract_indexes(cur, ["Voter"])
    assert [i.name for i in idxs] == ["Voter_LastName_idx", "Voter_family_idx"]
    assert idxs[1].where == '("Mailing_Families_FamilyID" IS NOT NULL)'
    assert idxs[1].unique is False


def test_extract_primary_keys() -> None:
    cur = _FakeCursor([("Voter", "Voter_pkey", 'PRIMARY KEY (id, "State")')])
    pks = extract_primary_keys(cur, ["Voter"])
    assert pks[0].constraint == "Voter_pkey"
    assert pks[0].columns == ["id", "State"]


def test_extract_foreign_keys() -> None:
    cur = _FakeCursor(
        [("DistrictVoter", "DistrictVoter_voter_fkey",
          'ALTER TABLE ONLY public."DistrictVoter" ADD CONSTRAINT "DistrictVoter_voter_fkey" '
          'FOREIGN KEY (voter_id) REFERENCES public."Voter"(id)')]
    )
    fks = extract_foreign_keys(cur, ["DistrictVoter"])
    assert fks[0].constraint == "DistrictVoter_voter_fkey"
    assert "FOREIGN KEY" in fks[0].sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_serving_structure.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/loader/people_api/schema/serving_structure.py
"""Extract the serving cluster's PK / indexes / FKs from pg_catalog.

These are Postgres-specific (Databricks marts have no notion of them). The records
seed schema_spec via the `extract-serving-structure` CLI; build_indexes consumes the
committed result. Functions take a cursor so they're unit-testable without a live DB.
"""

from __future__ import annotations

import re
from typing import Any

from loader.people_api.schema.index_specs import ForeignKey, IndexDef, PrimaryKey

_PK_COLS_RE = re.compile(r"PRIMARY KEY \((?P<cols>.+)\)")
_WHERE_RE = re.compile(r"\bWHERE\s+(?P<where>.+)$")
_UNIQUE_RE = re.compile(r"^CREATE UNIQUE INDEX", re.IGNORECASE)


def _split_cols(cols: str) -> list[str]:
    return [c.strip().strip('"') for c in cols.split(",")]


def extract_indexes(cur: Any, tables: list[str]) -> list[IndexDef]:
    """Non-constraint indexes (the PK/unique-constraint-backing indexes are excluded by the query)."""
    cur.execute(
        """
        SELECT t.relname AS table, c.relname AS index_name, pg_get_indexdef(i.indexrelid) AS def,
               i.indisunique AS is_unique
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public' AND t.relname = ANY(%s) AND NOT i.indisprimary
        ORDER BY t.relname, c.relname
        """,
        (tables,),
    )
    out: list[IndexDef] = []
    for table, name, definition, is_unique in cur.fetchall():
        where_m = _WHERE_RE.search(definition)
        out.append(
            IndexDef(
                table=table,
                name=name,
                sql=definition if definition.rstrip().endswith(";") else definition + ";",
                unique=bool(is_unique) or bool(_UNIQUE_RE.match(definition)),
                columns=[],  # manifest-only; the verbatim sql is what gets re-issued
                where=where_m.group("where").strip() if where_m else None,
            )
        )
    return out


def extract_primary_keys(cur: Any, tables: list[str]) -> list[PrimaryKey]:
    cur.execute(
        """
        SELECT t.relname AS table, con.conname AS constraint,
               pg_get_constraintdef(con.oid) AS def
        FROM pg_constraint con
        JOIN pg_class t ON t.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public' AND t.relname = ANY(%s) AND con.contype = 'p'
        ORDER BY t.relname
        """,
        (tables,),
    )
    out: list[PrimaryKey] = []
    for table, constraint, definition in cur.fetchall():
        m = _PK_COLS_RE.search(definition)
        cols = _split_cols(m.group("cols")) if m else []
        out.append(PrimaryKey(table=table, constraint=constraint, columns=cols))
    return out


def extract_foreign_keys(cur: Any, tables: list[str]) -> list[ForeignKey]:
    cur.execute(
        """
        SELECT t.relname AS table, con.conname AS constraint,
               'ALTER TABLE ONLY public.' || quote_ident(t.relname) ||
               ' ADD CONSTRAINT ' || quote_ident(con.conname) || ' ' ||
               pg_get_constraintdef(con.oid) AS def
        FROM pg_constraint con
        JOIN pg_class t ON t.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'public' AND t.relname = ANY(%s) AND con.contype = 'f'
        ORDER BY t.relname, con.conname
        """,
        (tables,),
    )
    return [
        ForeignKey(table=table, constraint=constraint, sql=definition.rstrip(";") + ";")
        for table, constraint, definition in cur.fetchall()
    ]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_serving_structure.py -q`
Expected: PASS (3 cases).

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/schema/serving_structure.py tests/test_serving_structure.py
git commit -m "feat(loader): extract serving PK/indexes/FKs from pg_catalog"
```

---

## Task 5: `extract-serving-structure` CLI + generated `_serving_seed.py`

**Files:**
- Modify: `src/loader/people_api/cli.py`
- Create (generator helper): `src/loader/people_api/schema/serving_seed_writer.py`
- Test: `tests/test_serving_seed_writer.py`

The command connects to prod (`connect_prod`), extracts the structure for all four tables, and writes a committed `schema/_serving_seed.py` containing literal `PrimaryKey`/`IndexDef`/`ForeignKey` lists. This is the one-time bootstrap ("inspect prod for the first run"); re-running it regenerates the file as a reviewable diff. We generate a file (not 266 hand-written records) so the data stays out of human hands.

- [ ] **Step 1: Write the failing test (the seed renderer is the testable unit)**

```python
# tests/test_serving_seed_writer.py
"""The seed writer renders importable Python literals for the dataclasses."""

from __future__ import annotations

from loader.people_api.schema.index_specs import IndexDef, PrimaryKey
from loader.people_api.schema.serving_seed_writer import render_seed_module


def test_render_seed_module_is_valid_python_with_records() -> None:
    pks = [PrimaryKey(table="Voter", constraint="Voter_pkey", columns=["id", "State"])]
    idxs = [IndexDef(table="Voter", name="Voter_x", sql='CREATE INDEX ...;', unique=False,
                     columns=[], where=None)]
    src = render_seed_module(pks, idxs, [])
    ns: dict = {}
    exec(compile(src, "_serving_seed.py", "exec"), ns)  # must be importable
    assert ns["PRIMARY_KEYS"][0].constraint == "Voter_pkey"
    assert ns["INDEXES"][0].name == "Voter_x"
    assert ns["FOREIGN_KEYS"] == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_serving_seed_writer.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Implement the seed writer**

```python
# src/loader/people_api/schema/serving_seed_writer.py
"""Render extracted serving-structure records into an importable, committed Python module.

GENERATED FILE WARNING is embedded so reviewers know `_serving_seed.py` is produced by
`loader extract-serving-structure`, not hand-edited."""

from __future__ import annotations

from loader.people_api.schema.index_specs import ForeignKey, IndexDef, PrimaryKey

_HEADER = '''\
"""GENERATED by `loader extract-serving-structure` — do not edit by hand.

Serving-cluster PK / indexes / FKs captured from pg_catalog. Regenerate to refresh."""

from loader.people_api.schema.index_specs import ForeignKey, IndexDef, PrimaryKey
'''


def render_seed_module(
    pks: list[PrimaryKey], idxs: list[IndexDef], fks: list[ForeignKey]
) -> str:
    lines = [_HEADER, ""]
    lines.append("PRIMARY_KEYS: list[PrimaryKey] = [")
    for p in pks:
        lines.append(f"    PrimaryKey(table={p.table!r}, constraint={p.constraint!r}, columns={p.columns!r}),")
    lines.append("]")
    lines.append("")
    lines.append("INDEXES: list[IndexDef] = [")
    for i in idxs:
        lines.append(
            f"    IndexDef(table={i.table!r}, name={i.name!r}, sql={i.sql!r}, "
            f"unique={i.unique!r}, columns={i.columns!r}, where={i.where!r}),"
        )
    lines.append("]")
    lines.append("")
    lines.append("FOREIGN_KEYS: list[ForeignKey] = [")
    for f in fks:
        lines.append(f"    ForeignKey(table={f.table!r}, constraint={f.constraint!r}, sql={f.sql!r}),")
    lines.append("]")
    lines.append("")
    return "\n".join(lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_serving_seed_writer.py -q`
Expected: PASS.

- [ ] **Step 5: Wire the CLI command**

In `src/loader/people_api/cli.py`, add (mirroring the `inspect-prod` command's `_setup`/`connect_prod` usage):

```python
@app.command(name="extract-serving-structure")
def extract_serving_structure() -> None:
    """Bootstrap: capture prod PK/indexes/FKs into schema/_serving_seed.py (committed)."""
    from pathlib import Path

    from loader.people_api.db import connect_prod
    from loader.people_api.schema import serving_structure as ss
    from loader.people_api.schema.serving_seed_writer import render_seed_module

    cfg = _setup()
    tables = ["Voter", "District", "DistrictStats", "DistrictVoter"]
    with connect_prod(cfg) as conn, conn.cursor() as cur:
        pks = ss.extract_primary_keys(cur, tables)
        idxs = ss.extract_indexes(cur, tables)
        fks = ss.extract_foreign_keys(cur, tables)
    out = Path(__file__).parent / "schema" / "_serving_seed.py"
    out.write_text(render_seed_module(pks, idxs, fks), encoding="utf-8")
    typer.echo(f"wrote {out} ({len(pks)} PKs, {len(idxs)} indexes, {len(fks)} FKs)")
```

- [ ] **Step 6: Create an importable placeholder seed (committed)**

So `schema_spec` (Task 7) and CI import cleanly before the prod bootstrap (Task 6) runs, write `src/loader/people_api/schema/_serving_seed.py` with empty lists, using the writer to guarantee the exact shape:

Run:
```bash
uv run python -c "
from pathlib import Path
from loader.people_api.schema.serving_seed_writer import render_seed_module
Path('src/loader/people_api/schema/_serving_seed.py').write_text(render_seed_module([], [], []))
"
```
Tests patch `indexes_for`/`primary_key_for`, so an empty seed keeps CI green; the bootstrap (Task 6) overwrites it with real prod data.

- [ ] **Step 7: Run + commit (the command itself needs prod; the renderer + placeholder are covered)**

Run: `uv run pytest tests/test_serving_seed_writer.py -q && uv run ty check`
Expected: PASS / clean.

```bash
git add src/loader/people_api/cli.py src/loader/people_api/schema/serving_seed_writer.py src/loader/people_api/schema/_serving_seed.py tests/test_serving_seed_writer.py
git commit -m "feat(loader): extract-serving-structure CLI writes committed seed"
```

---

## Task 6: Bootstrap run — generate `_serving_seed.py` from prod

**Files:**
- Create (generated): `src/loader/people_api/schema/_serving_seed.py`

This is a procedural task (runs against live prod, requires SSO + `connect_prod`). No new code.

- [ ] **Step 1: Authenticate** — `aws sso login` if the token is stale (the loader's `connect_prod` reads the SSM connection string for the env).

- [ ] **Step 2: Run the extractor**

Run: `LOADER_ENV=prod uv run loader extract-serving-structure`
Expected: prints `wrote .../schema/_serving_seed.py (N PKs, ~266 indexes, M FKs)`.

- [ ] **Step 3: Sanity-check the output** — open `_serving_seed.py`; confirm `INDEXES` is populated (hundreds for Voter incl. partial `WHERE` clauses), `PRIMARY_KEYS` has `Voter_pkey`, and the file imports cleanly: `uv run python -c "from loader.people_api.schema import _serving_seed as s; print(len(s.INDEXES))"`.

- [ ] **Step 4: Commit the generated seed**

```bash
git add src/loader/people_api/schema/_serving_seed.py
git commit -m "chore(loader): seed serving structure from prod (generated)"
```

---

## Task 7: Declarative schema spec

**Files:**
- Create: `src/loader/people_api/schema/schema_spec.py`
- Test: `tests/test_schema_spec.py`

Composes the static structure (partition strategy, type overrides, PG↔mart mapping) with the generated `_serving_seed.py`. `id` is the documented type override (mart emits a salted-uuid `string`; PG wants `UUID`).

- [ ] **Step 1: Write the failing test**

```python
# tests/test_schema_spec.py
"""schema_spec exposes per-table specs that compose marts + the serving seed."""

from __future__ import annotations

from loader.people_api.schema.schema_spec import TABLE_SPECS, indexes_for, primary_key_for


def test_specs_cover_four_tables_with_overrides_and_partition() -> None:
    assert set(TABLE_SPECS) == {"Voter", "District", "DistrictStats", "DistrictVoter"}
    voter = TABLE_SPECS["Voter"]
    assert voter.partition_by == "State"
    assert voter.type_overrides["id"] == "UUID"
    assert TABLE_SPECS["District"].partition_by is None


def test_lookup_helpers_filter_seed_by_table() -> None:
    # These read from the committed _serving_seed; assert they return only the table asked for.
    assert all(p.table == "Voter" for p in [primary_key_for("Voter")] if primary_key_for("Voter"))
    assert all(i.table == "Voter" for i in indexes_for("Voter"))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_schema_spec.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/loader/people_api/schema/schema_spec.py
"""Declarative target-schema spec: the serving structure the marts cannot describe.

Columns/types come from the marts (mart_introspect); this module owns the Postgres-side
decisions — partitioning, per-column type overrides, and (via the generated _serving_seed)
the PK/indexes/FKs. build_indexes and emit_ddl read from here."""

from __future__ import annotations

from dataclasses import dataclass, field

from loader.people_api.schema import _serving_seed as seed
from loader.people_api.schema.index_specs import ForeignKey, IndexDef, PrimaryKey


@dataclass(frozen=True, slots=True)
class TableSpec:
    pg_table: str
    partition_by: str | None  # column name for LIST partitioning, or None for a plain table
    type_overrides: dict[str, str] = field(default_factory=dict)


TABLE_SPECS: dict[str, TableSpec] = {
    "Voter": TableSpec(pg_table="Voter", partition_by="State", type_overrides={"id": "UUID"}),
    "District": TableSpec(pg_table="District", partition_by=None, type_overrides={"id": "UUID"}),
    "DistrictStats": TableSpec(pg_table="DistrictStats", partition_by=None),
    "DistrictVoter": TableSpec(
        pg_table="DistrictVoter",
        partition_by=None,
        type_overrides={"voter_id": "UUID", "district_id": "UUID"},
    ),
}


def primary_key_for(table: str) -> PrimaryKey | None:
    matches = [p for p in seed.PRIMARY_KEYS if p.table == table]
    return matches[0] if matches else None


def indexes_for(table: str) -> list[IndexDef]:
    return [i for i in seed.INDEXES if i.table == table]


def foreign_keys_for(table: str) -> list[ForeignKey]:
    return [f for f in seed.FOREIGN_KEYS if f.table == table]
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_schema_spec.py -q`
Expected: PASS (requires Task 6's `_serving_seed.py` to exist).

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/schema/schema_spec.py tests/test_schema_spec.py
git commit -m "feat(loader): declarative people_api schema spec"
```

---

## Task 8: `emit-ddl` renderer + CLI

**Files:**
- Create: `src/loader/people_api/schema/emit_ddl.py`
- Modify: `src/loader/people_api/cli.py`
- Test: `tests/test_emit_ddl.py`

Renders one `CREATE TABLE public."<name>" ( ... );` block per table in pg_dump shape (so the existing `extract_create_tables` parser reads it). Columns from a `MartColumn` list (injected — test passes a fixed list; the CLI calls `introspect_mart`), types via `to_pg_type` + spec overrides.

- [ ] **Step 1: Write the failing test (golden render)**

```python
# tests/test_emit_ddl.py
"""emit_ddl renders pg_dump-shaped CREATE TABLE blocks from mart columns + spec overrides."""

from __future__ import annotations

from loader.people_api.schema.emit_ddl import render_create_table
from loader.people_api.schema.mart_introspect import MartColumn
from loader.people_api.schema.schema_spec import TableSpec


def test_render_applies_types_and_overrides_and_quotes() -> None:
    cols = [
        MartColumn(name="id", spark_type="string", nullable=False),
        MartColumn(name="State", spark_type="string", nullable=False),
        MartColumn(name="Age_Int", spark_type="int", nullable=True),
    ]
    spec = TableSpec(pg_table="Voter", partition_by="State", type_overrides={"id": "UUID"})
    ddl = render_create_table(spec, cols)
    assert ddl == (
        'CREATE TABLE public."Voter" (\n'
        '    "id" UUID NOT NULL,\n'
        '    "State" TEXT NOT NULL,\n'
        '    "Age_Int" INTEGER\n'
        ");"
    )


def test_render_round_trips_through_extract_create_tables() -> None:
    from loader.people_api.schema.table_ddl import extract_column_names, extract_create_tables

    cols = [
        MartColumn(name="id", spark_type="string", nullable=False),
        MartColumn(name="name", spark_type="string", nullable=True),
    ]
    spec = TableSpec(pg_table="District", partition_by=None, type_overrides={"id": "UUID"})
    ddl = render_create_table(spec, cols)
    parsed = extract_create_tables(ddl)
    assert "District" in parsed
    assert extract_column_names(parsed["District"]) == ["id", "name"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_emit_ddl.py -q`
Expected: FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: Write the implementation**

```python
# src/loader/people_api/schema/emit_ddl.py
"""Render target_schema.sql from mart columns + the schema spec.

Output is pg_dump-shaped CREATE TABLE blocks (no indexes/PK — those live in schema_spec
and are applied by build_indexes), so the existing table_ddl.extract_create_tables parser
reads it unchanged. Deterministic: same marts + same spec -> identical bytes."""

from __future__ import annotations

from loader.people_api.config import LoaderConfig
from loader.people_api.schema.mart_introspect import MartColumn, introspect_mart
from loader.people_api.schema.schema_spec import TABLE_SPECS, TableSpec
from loader.people_api.schema.type_map import to_pg_type


def _column_type(spec: TableSpec, col: MartColumn) -> str:
    return spec.type_overrides.get(col.name) or to_pg_type(col.spark_type)


def render_create_table(spec: TableSpec, columns: list[MartColumn]) -> str:
    lines = [f'CREATE TABLE public."{spec.pg_table}" (']
    rendered = []
    for col in columns:
        nn = "" if col.nullable else " NOT NULL"
        rendered.append(f'    "{col.name}" {_column_type(spec, col)}{nn}')
    return "\n".join([lines[0], ",\n".join(rendered) + "\n);"])


def render_target_schema(cfg: LoaderConfig) -> str:
    """Introspect every mart and render the full target_schema.sql contents."""
    blocks = []
    for pg_table, spec in TABLE_SPECS.items():
        columns = introspect_mart(cfg.mart_fqns[pg_table])
        blocks.append(render_create_table(spec, columns))
    return "\n\n".join(blocks) + "\n"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_emit_ddl.py -q`
Expected: PASS (2 cases).

- [ ] **Step 5: Wire the CLI command**

In `cli.py`:

```python
@app.command(name="emit-ddl")
def emit_ddl() -> None:
    """Render schema/data/target_schema.sql from the people_api marts (committed artifact)."""
    from pathlib import Path

    from loader.people_api.schema.emit_ddl import render_target_schema

    cfg = _setup(verify_aws=False)
    out = Path(__file__).parent / "schema" / "data" / "target_schema.sql"
    out.write_text(render_target_schema(cfg), encoding="utf-8")
    typer.echo(f"wrote {out}")
```

- [ ] **Step 6: Run + commit**

Run: `uv run pytest tests/test_emit_ddl.py -q && uv run ty check`
Expected: PASS / clean.

```bash
git add src/loader/people_api/schema/emit_ddl.py src/loader/people_api/cli.py tests/test_emit_ddl.py
git commit -m "feat(loader): emit-ddl renders target_schema.sql from marts"
```

---

## Task 9: Bootstrap run — generate `target_schema.sql` + parity check

**Files:**
- Create (generated): `src/loader/people_api/schema/data/target_schema.sql`

Procedural (needs Databricks creds: `DATABRICKS_HOST` + token/profile).

- [ ] **Step 1: Generate** — `uv run loader emit-ddl` → writes `schema/data/target_schema.sql`.

- [ ] **Step 2: Parity check vs the dump.** Compare the generated `CREATE TABLE public."Voter"` columns to the prod dump's, to surface intended/unintended differences:

Run:
```bash
uv run python -c "
from loader.people_api.schema.snapshot import load_prod_dump
from loader.people_api.schema.table_ddl import extract_create_tables, extract_column_names
from pathlib import Path
dump = extract_create_tables(load_prod_dump(None, None))
gen = extract_create_tables(Path('src/loader/people_api/schema/data/target_schema.sql').read_text())
old, new = extract_column_names(dump['Voter']), extract_column_names(gen['Voter'])
print('only in prod_dump:', sorted(set(old) - set(new)))
print('only in generated:', sorted(set(new) - set(old)))
"
```
Expected: the two sets are equal, OR every difference is an intended mart change (the known renames/additions in PLAN_LOADER "Column Scoping"). Record any intended differences in the commit message.

- [ ] **Step 3: Commit the generated schema**

```bash
git add src/loader/people_api/schema/data/target_schema.sql
git commit -m "chore(loader): generate target_schema.sql from marts (generated)"
```

---

# PHASE 2 — cut over the consumers and retire the dump

## Task 10: `create_schema` reads `target_schema.sql`, creates all four tables

**Files:**
- Modify: `src/loader/people_api/steps/create_schema.py`
- Modify: `src/loader/people_api/schema/snapshot.py` (add `load_target_schema`)
- Modify: `tests/test_create_schema.py`

- [ ] **Step 1: Add the loader in `snapshot.py`**

```python
def load_target_schema(cfg: LoaderConfig, run_date: str) -> str:
    """Read the committed, mart-generated target schema (replaces load_prod_dump)."""
    del cfg, run_date
    override = os.environ.get("LOADER_TARGET_SCHEMA_PATH")
    path = Path(override) if override else DATA_DIR / "target_schema.sql"
    return path.read_text(encoding="utf-8")
```

- [ ] **Step 2: Update `create_schema.run` to iterate all four tables**

Replace the Voter-only extraction with a loop over `TABLE_SPECS`. For `Voter` (partition_by set) use `build_partitioned_ddl`; for the others apply the plain CREATE TABLE. Full replacement of the body between `load_*` and the manifest write:

```python
from loader.people_api.schema.snapshot import load_target_schema
from loader.people_api.schema.schema_spec import TABLE_SPECS
# ...
    schema_sql = load_target_schema(cfg, run_date)
    tables = extract_create_tables(schema_sql)
    missing = [t for t in TABLE_SPECS if t not in tables]
    if missing:
        raise RuntimeError(f"target_schema.sql missing tables: {missing} (found {sorted(tables)})")

    statements: list[str] = []
    tables_created: list[str] = []
    for name, spec in TABLE_SPECS.items():
        if spec.partition_by:
            parent, children = build_partitioned_ddl(tables[name], name, STATES)
            statements.extend([parent, *children])
            tables_created.extend([name, *[f"{name}_{s}" for s in STATES]])
        else:
            create = tables[name]
            create = create.replace(
                f'CREATE TABLE public."{name}"', f'CREATE TABLE IF NOT EXISTS public."{name}"', 1
            )
            statements.append(create)
            tables_created.append(name)

    full_ddl = "\n".join(statements)
    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", full_ddl)

    with connect_new(cfg, run_date) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        for stmt in statements:
            with conn.cursor() as cur:
                cur.execute(stmt)  # ty: ignore[no-matching-overload]
```

Update the `SchemaManifest(... tables_created=tables_created ...)`. Drop the `_TARGET_TABLE` guard / single-table assumptions.

- [ ] **Step 3: Update `tests/test_create_schema.py`** to set `LOADER_TARGET_SCHEMA_PATH` to a tmp file containing CREATE TABLE blocks for all four tables (Voter with `State`, the others plain), and assert `tables_created` includes the four parents + Voter's per-state children. Use `monkeypatch.setenv` and `tmp_path`.

- [ ] **Step 4: Run**

Run: `uv run pytest tests/test_create_schema.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/loader/people_api/steps/create_schema.py src/loader/people_api/schema/snapshot.py tests/test_create_schema.py
git commit -m "feat(loader): create_schema builds all four people_api tables from target_schema.sql"
```

---

## Task 11: `build_indexes` reads `schema_spec`, all four tables

**Files:**
- Modify: `src/loader/people_api/steps/build_indexes.py`
- Modify: `tests/test_build_indexes.py`

- [ ] **Step 1: Replace the dump parse with spec lookups.** Swap the `load_prod_dump` + `parse_primary_keys`/`parse_indexes` block for reads from `schema_spec`, iterating all four tables. The `State`-in-PK enforcement stays but only for partitioned tables:

```python
from loader.people_api.schema.schema_spec import TABLE_SPECS, indexes_for, primary_key_for
# ...
    pks: list[PrimaryKey] = []
    idxs: list[IndexDef] = []
    for name, spec in TABLE_SPECS.items():
        pk = primary_key_for(name)
        if pk is not None:
            cols = list(dict.fromkeys([*pk.columns, "State"])) if spec.partition_by else pk.columns
            pks.append(PrimaryKey(table=pk.table, constraint=pk.constraint, columns=cols))
        idxs.extend(indexes_for(name))
    log.info("indexes.parsed", primary_keys=len(pks), indexes=len(idxs))
```

Leave `_add_primary_key` / `_create_index` / `_build_in_parallel` / `_analyze` and FK handling unchanged in shape. Remove the `from loader.people_api.schema.snapshot import load_prod_dump` and `parse_*` imports; remove the `_TARGET_TABLE` filter.

- [ ] **Step 2: Update `tests/test_build_indexes.py`** to monkeypatch `schema_spec.primary_key_for` / `indexes_for` (or patch `step.primary_key_for` / `step.indexes_for`) to return fixed records, instead of feeding a dump. Assert PK gets `State` appended for Voter and not for a non-partitioned table.

- [ ] **Step 3: Run**

Run: `uv run pytest tests/test_build_indexes.py -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/loader/people_api/steps/build_indexes.py tests/test_build_indexes.py
git commit -m "feat(loader): build_indexes reads PK/indexes from schema_spec"
```

---

## Task 12: inspect-prod also emits the serving-structure artifact (drift signal)

**Files:**
- Modify: `src/loader/people_api/steps/inspect_prod.py`
- Modify: `tests/test_inspect_prod.py`

Per-run, inspect-prod writes the live serving structure as an S3 artifact and logs a warning if it diverges from the committed seed (so a prod index added out-of-band is visible). It does NOT rewrite the committed seed (that's the deliberate `extract-serving-structure` action).

- [ ] **Step 1: In `inspect_prod.run`,** after the existing inspection, extract indexes via `serving_structure.extract_indexes(cur, tables)` and compare names to `schema_spec.indexes_for`; `log.warning("inspect.index_drift", added=..., removed=...)` on difference, and `put_artifact(cfg, run_date, "schema/serving_structure.json", ...)`. Reuse the open `connect_prod` cursor.

- [ ] **Step 2: Add a test** to `tests/test_inspect_prod.py` that feeds a fake cursor returning one extra index and asserts the drift warning fires (capture via `caplog` or a structlog capture, matching the file's existing logging-assertion style).

- [ ] **Step 3: Run**

Run: `uv run pytest tests/test_inspect_prod.py -q`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/loader/people_api/steps/inspect_prod.py tests/test_inspect_prod.py
git commit -m "feat(loader): inspect-prod warns on serving-index drift vs the seed"
```

---

## Task 13: Retire `prod_dump.sql` and the dead parse path

**Files:**
- Delete: `src/loader/people_api/schema/data/prod_dump.sql`
- Modify: `src/loader/people_api/schema/snapshot.py` (remove `load_prod_dump`)
- Modify: `src/loader/people_api/schema/index_specs.py` (remove `parse_primary_keys`/`parse_indexes`/`parse_foreign_keys` + their regex/helpers; keep the dataclasses)
- Modify: `docs/PLAN_LOADER.md` (note the dump is retired; schema now generated)

- [ ] **Step 1: Confirm no remaining references.**

Run: `grep -rn "load_prod_dump\|parse_primary_keys\|parse_indexes\|parse_foreign_keys\|prod_dump" src/ tests/`
Expected: only `serving_structure`/`schema_spec` paths and the (now removed) test references remain. Fix any straggler (e.g. `test_integration_pg.py` uses `extract_create_tables` on an inline DDL string — that stays; it does not read prod_dump).

- [ ] **Step 2: Delete + prune.**

```bash
git rm src/loader/people_api/schema/data/prod_dump.sql
```
Remove `load_prod_dump` from `snapshot.py` (keep `load_target_schema`; if the file is now just that one function, that's fine). Remove the `parse_*` functions and their private regex/`_extract_balanced`/`_split_top_level`/`_parse_*` helpers from `index_specs.py`; keep `PrimaryKey`/`IndexDef`/`ForeignKey`.

- [ ] **Step 3: Update `docs/PLAN_LOADER.md`** — add a line under the superseded banner: schema is now generated (`target_schema.sql` from marts + `_serving_seed.py` from prod via `extract-serving-structure`); `prod_dump.sql` removed.

- [ ] **Step 4: Full suite + type + lint.**

Run: `uv run pytest -q && uv run ty check && uv run ruff check . && uv run ruff format --check .`
Expected: all green (no test reads `prod_dump.sql`).

- [ ] **Step 5: Commit**

```bash
git add -A src/loader/people_api/schema docs/PLAN_LOADER.md
git commit -m "refactor(loader): retire prod_dump.sql; schema generated from marts + seed"
```

---

## Task 14: Final verification + push

- [ ] **Step 1: Full local gate.**

Run: `uv run pytest -q && uv run ty check && uv run ruff check . && uv run ruff format --check .`
Then from the repo root: `pre-commit run --all-files`
Expected: all pass.

- [ ] **Step 2: Push and let the PR #504 checks + delegate-reviewer run.**

```bash
git push
```

- [ ] **Step 3: Address any delegate-reviewer findings** per the established loop (reply to each, `delegate review` to retrigger).

---

## Notes for the implementer

- **Databricks creds:** `emit-ddl` and the bootstrap need `DATABRICKS_HOST` + a token/profile (see `.env.example`). `extract-serving-structure` needs prod DB access via `connect_prod` (SSM connection string + `aws sso login`). These two bootstrap tasks (6 and 9) can't run in CI — they're operator-run, and their outputs are committed.
- **Dependency on the unload (DATA-1908):** the generated DDL is mart-shaped. The `copy` step must load columns matching the marts. If the unload still reads `int__l2_nationwide_uniform` (L2-native names), the columns won't line up — track aligning the unload to the marts separately (called out in the spec's Non-goals).
- **Open items the bootstrap resolves empirically:** the exact dbt schema for the marts (Task 3 default may need `LOADER_MART_CATALOG_SCHEMA`), and whether any non-Voter table is large enough to warrant partitioning (Task 7 leaves them plain; revisit if `DistrictVoter` is huge).
