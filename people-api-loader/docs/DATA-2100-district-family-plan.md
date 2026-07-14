# District family loader — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the People API loader from loading only `Voter` to loading the full serving set — `District`, `DistrictStats`, `DistrictVoter`, and `Voter` — into the freshly-provisioned Aurora cluster on every refresh.

**Architecture:** The pipeline substrate is already table-generic (`emit_ddl` iterates `TABLE_SPECS`, `serving_structure.extract_*` take a table list, `config._MART_MODELS` maps all four marts). The work is to grow `TABLE_SPECS` from one entry to four, make the dead `TableSpec.partition_by` field the live signal for "LIST-partitioned by State vs flat", and replace each `_TARGET_TABLE="Voter"` / literal `"State"` / `Voter_<state>` chokepoint in the step modules with iteration over the specs keyed on that flag. No foreign keys are created (serving enforces none). The design doc is `docs/DATA-2100-district-family-design.md`.

**Tech Stack:** Python 3.14, uv, ruff (line length 110), ruff-format, ty (pinned), pytest, psycopg3, boto3, Databricks SQL. Pydantic manifests.

## Global Constraints

- **Astral toolchain must stay green:** `uv run ruff check`, `uv run ruff format --check`, `uv run ty check`, `uv run pytest` all pass. Line length 110. Do not introduce black/isort/mypy/pyright.
- **Voter behavior is preserved exactly.** Every generalization must reproduce today's Voter SQL/paths byte-for-byte for the Voter table. The existing Voter assertions in the test suite must stay green (adjust only the parts that legitimately move, e.g. an S3 path that now carries a `Voter/` segment — and where a path changes, update the test in the same task).
- **No foreign keys.** Serving enforces none (`_serving_seed.FOREIGN_KEYS == []`); the loader creates none.
- **Partitioning:** `Voter` and `DistrictVoter` are LIST-partitioned by `"State"`; `District` and `DistrictStats` are flat. The partition key must be in the PK of a partitioned table, so `DistrictVoter`'s PK becomes `(district_id, voter_id, State)`.
- **Cutover (DATA-1855) is out of scope.** Do NOT edit any `dbt/project/models/write/*` `ON CONFLICT` clauses; do NOT delete the legacy `load_people_api_old` DAG or the dbt DistrictVoter write path in this plan's code tasks — those land at cutover (see Task 8, which only documents the follow-up).
- **Two committed artifacts are regenerated deployment-side, not by implementers:** `schema/data/target_schema.sql` (via `loader emit-ddl`, needs Databricks) and `schema/_serving_seed.py` (via `loader extract-serving-structure`, needs prod). Implementers must NOT hand-edit `_serving_seed.py`. Tests supply their own DDL/spec fixtures.
- **Core stays consumer-agnostic:** no people-api specifics in `src/loader/core/`.
- **DistrictStats `buckets`:** stored `jsonb`; the two non-camelCase struct keys are renamed in the unload SELECT (self-contained; no dbt dependency).

## File Structure

- `schema/schema_spec.py` — grows `TABLE_SPECS` to 4; `TableSpec` gains an optional `primary_key`; `primary_key_for` falls back to it; new `is_partitioned(table)` / `partition_column(table)` helpers.
- `schema/emit_ddl.py` — no logic change; tests extended.
- `steps/create_schema.py` — iterate specs; partitioned vs flat DDL.
- `manifests.py` — `UnloadManifest` becomes per-table (`tables: list[UnloadTable]`); `UnloadFile` gains `table`.
- `schema/unload_sql.py` — flat-table statement variants; per-table `select_exprs` transform hook.
- `steps/unload.py` — iterate specs; partitioned vs flat unload; buckets transform; write the new manifest shape; update `copy.py`/`validate.py` reads to the new shape (contract change is atomic here).
- `steps/copy_s3.py` — iterate specs; table in the advisory-lock key; flat-table copy path.
- `steps/build_indexes.py` — iterate target tables; parametrize the partition literals; State-append only for partitioned; flat PK/index at parent level; ANALYZE each.
- `steps/validate.py` — per-state gates for partitioned tables, whole-table gates for flat.
- Each `tests/test_*.py` for the above.

---

## Task 1: Table specs foundation

**Files:**
- Modify: `src/loader/people_api/schema/schema_spec.py`
- Test: `tests/test_schema_spec.py`

**Interfaces:**
- Consumes: `index_specs.PrimaryKey`, `_serving_seed` lists.
- Produces: `TABLE_SPECS` (4 entries), `TableSpec.primary_key: PrimaryKey | None`, `primary_key_for(table)` with spec fallback, `is_partitioned(table) -> bool`, `partition_column(table) -> str | None`.

- [ ] **Step 1: Write failing tests** in `tests/test_schema_spec.py`:

```python
from __future__ import annotations

from loader.people_api.schema import schema_spec as ss


def test_all_four_tables_specced() -> None:
    assert set(ss.TABLE_SPECS) == {"Voter", "District", "DistrictStats", "DistrictVoter"}


def test_partition_flags() -> None:
    assert ss.is_partitioned("Voter") is True
    assert ss.is_partitioned("DistrictVoter") is True
    assert ss.is_partitioned("District") is False
    assert ss.is_partitioned("DistrictStats") is False
    assert ss.partition_column("DistrictVoter") == "State"
    assert ss.partition_column("District") is None


def test_districtstats_spec() -> None:
    spec = ss.TABLE_SPECS["DistrictStats"]
    assert spec.partition_by is None
    assert spec.type_overrides.get("buckets") == "jsonb"
    # not a serving table -> PK carried on the spec, not the seed
    assert spec.primary_key is not None
    assert spec.primary_key.columns == ["district_id"]


def test_primary_key_for_spec_fallback() -> None:
    # DistrictStats is absent from _serving_seed; the spec PK is returned.
    pk = ss.primary_key_for("DistrictStats")
    assert pk is not None and pk.columns == ["district_id"]


def test_primary_key_for_seed_wins_when_present() -> None:
    # District IS in the seed; spec carries no PK, seed value is used.
    pk = ss.primary_key_for("District")
    assert pk is not None and pk.columns == ["id"]
```

- [ ] **Step 2: Run — expect FAIL** (`is_partitioned`/new specs missing):

Run: `cd people-api-loader && uv run pytest tests/test_schema_spec.py -q`
Expected: FAIL (AttributeError / KeyError).

- [ ] **Step 3: Implement.** Replace the `TableSpec` dataclass, scope comment, `TABLE_SPECS`, and `primary_key_for` in `schema_spec.py`; add the two helpers. Full target for the changed regions:

```python
@dataclass(frozen=True, slots=True)
class TableSpec:
    pg_table: str
    partition_by: str | None  # column name for LIST partitioning, or None for a plain table
    type_overrides: dict[str, str] = field(default_factory=dict)
    # App/Prisma-managed columns that exist in the serving table but not the mart, appended
    # as (name, pg_type, nullable). The mart is the source for everything else.
    extra_columns: list[tuple[str, str, bool]] = field(default_factory=list)
    # PK for tables the serving snapshot cannot describe (e.g. DistrictStats is not yet a serving
    # Postgres table, so _serving_seed has no entry). For serving tables this stays None and the
    # seed is authoritative. NEVER hand-edit _serving_seed.py; this is the escape hatch.
    primary_key: PrimaryKey | None = None


# The loader bulk-loads the full serving set onto the fresh cluster: Voter and DistrictVoter are
# LIST-partitioned by "State" (large, per-partition parallel index builds); District and
# DistrictStats are flat. Columns/types come from the marts (mart_introspect); this module owns the
# Postgres-side decisions. DistrictStats is not yet a serving table, so its PK is carried here, not
# in the generated _serving_seed. Serving enforces no FKs, so none are created. Voter's single
# Prisma-only column the mart omits is Mailing_HHGender_Description; `id` is the mart's salted-uuid
# string stored as UUID.
TABLE_SPECS: dict[str, TableSpec] = {
    "Voter": TableSpec(
        pg_table="Voter",
        partition_by="State",
        type_overrides={"id": "UUID"},
        extra_columns=[("Mailing_HHGender_Description", "TEXT", True)],
    ),
    "District": TableSpec(
        pg_table="District",
        partition_by=None,
    ),
    "DistrictStats": TableSpec(
        pg_table="DistrictStats",
        partition_by=None,
        type_overrides={"buckets": "jsonb"},
        primary_key=PrimaryKey(
            table="DistrictStats", constraint="DistrictStats_pkey", columns=["district_id"]
        ),
    ),
    "DistrictVoter": TableSpec(
        pg_table="DistrictVoter",
        partition_by="State",
    ),
}


def is_partitioned(table: str) -> bool:
    return TABLE_SPECS[table].partition_by is not None


def partition_column(table: str) -> str | None:
    return TABLE_SPECS[table].partition_by


def primary_key_for(table: str) -> PrimaryKey | None:
    matches = [p for p in seed.PRIMARY_KEYS if p.table == table]
    if matches:
        return matches[0]
    return TABLE_SPECS[table].primary_key if table in TABLE_SPECS else None
```

> Implementer note: `type_overrides` keys are mart column names. Confirm the mart's stats column is literally `buckets` (it is, per `m_people_api__districtstats`); if the mart column differs, override that name.

- [ ] **Step 4: Run — expect PASS.** Run: `cd people-api-loader && uv run pytest tests/test_schema_spec.py -q` → PASS.

- [ ] **Step 5: Toolchain + commit.**

```bash
cd people-api-loader && uv run ruff check . && uv run ruff format --check . && uv run ty check
git add src/loader/people_api/schema/schema_spec.py tests/test_schema_spec.py
git commit -m "[DATA-2100] schema_spec: add District family specs + partition helpers"
```

---

## Task 2: emit_ddl multi-table coverage

**Files:**
- Modify (only if a bug surfaces): `src/loader/people_api/schema/emit_ddl.py`
- Test: `tests/test_emit_ddl.py`

`render_target_schema` already iterates `TABLE_SPECS`; this task proves it renders all four tables correctly and honors the DistrictStats `buckets → jsonb` override, so the deployment-side `emit-ddl` regen produces the right artifact.

**Interfaces:**
- Consumes: `TABLE_SPECS`, `introspect_mart` (mocked in tests), `to_pg_type`.
- Produces: no new interface; a verified `target_schema.sql` renderer.

- [ ] **Step 1: Write failing tests** in `tests/test_emit_ddl.py` (follow the existing file's `introspect_mart` mock pattern — read it first). Add:

```python
def test_renders_all_four_tables(monkeypatch) -> None:
    from loader.people_api.schema import emit_ddl
    from loader.people_api.schema.mart_introspect import MartColumn

    marts = {
        "Voter": [MartColumn(name="id", spark_type="string", nullable=False),
                  MartColumn(name="State", spark_type="string", nullable=False)],
        "District": [MartColumn(name="id", spark_type="string", nullable=False),
                     MartColumn(name="state", spark_type="string", nullable=False)],
        "DistrictStats": [MartColumn(name="district_id", spark_type="string", nullable=False),
                          MartColumn(name="buckets", spark_type="struct<...>", nullable=True)],
        "DistrictVoter": [MartColumn(name="district_id", spark_type="string", nullable=False),
                          MartColumn(name="voter_id", spark_type="string", nullable=False),
                          MartColumn(name="state", spark_type="string", nullable=False)],
    }
    # introspect_mart is called with the FQN; map back via the tail model name -> pg table.
    fqn_to_pg = {f"cat.schema.m_people_api__{k.lower()}": k for k in
                 {"Voter": "voter", "District": "district",
                  "DistrictStats": "districtstats", "DistrictVoter": "districtvoter"}}
    # Simpler: patch introspect_mart to dispatch on the fqn substring.
    def fake_introspect(fqn: str):
        for pg, model in {"Voter": "voter", "District": "district",
                          "DistrictStats": "districtstats", "DistrictVoter": "districtvoter"}.items():
            if fqn.endswith(model):
                return marts[pg]
        raise AssertionError(fqn)
    monkeypatch.setattr(emit_ddl, "introspect_mart", fake_introspect)

    from types import SimpleNamespace
    cfg = SimpleNamespace(mart_fqns={
        "Voter": "cat.schema.m_people_api__voter",
        "District": "cat.schema.m_people_api__district",
        "DistrictStats": "cat.schema.m_people_api__districtstats",
        "DistrictVoter": "cat.schema.m_people_api__districtvoter",
    })
    sql = emit_ddl.render_target_schema(cfg)  # type: ignore[arg-type]
    assert 'CREATE TABLE public."Voter" (' in sql
    assert 'CREATE TABLE public."District" (' in sql
    assert 'CREATE TABLE public."DistrictStats" (' in sql
    assert 'CREATE TABLE public."DistrictVoter" (' in sql
    # buckets override -> jsonb (case as written in schema_spec)
    assert '"buckets" jsonb' in sql
```

> Implementer: adapt imports/`MartColumn` construction to the real `mart_introspect.MartColumn` signature (read `schema/mart_introspect.py`). The intent: four CREATE TABLEs render and the `buckets` column emits the `jsonb` override, not the mapped struct type.

- [ ] **Step 2: Run — expect FAIL** (only Voter renders today because `TABLE_SPECS` had one entry — but Task 1 already added the others, so this may PASS immediately; if so, keep it as a regression guard and note it). Run: `uv run pytest tests/test_emit_ddl.py -q`.

- [ ] **Step 3: Implement** — likely no change to `emit_ddl.py`. If the test reveals the `buckets` override isn't applied, verify `_column_type` reads `spec.type_overrides` (it does). Fix only a real defect.

- [ ] **Step 4: Run — expect PASS.**

- [ ] **Step 5: Toolchain + commit.**

```bash
cd people-api-loader && uv run ruff check . && uv run ruff format --check . && uv run ty check
git add src/loader/people_api/schema/emit_ddl.py tests/test_emit_ddl.py
git commit -m "[DATA-2100] emit_ddl: cover all four tables + buckets jsonb override"
```

---

## Task 3: create_schema — partitioned + flat tables

**Files:**
- Modify: `src/loader/people_api/steps/create_schema.py`
- Test: `tests/test_create_schema.py`

**Interfaces:**
- Consumes: `TABLE_SPECS`, `is_partitioned`, `partition_column`, `extract_create_tables`, `STATES`.
- Produces: a `create_schema.run` that creates every table in `TABLE_SPECS` present in the DDL — partitioned ones with a LIST-partition parent + per-state children, flat ones with a plain CREATE.

- [ ] **Step 1: Write failing tests.** Extend `tests/test_create_schema.py`. Add flat + multi-table DDL and assert both shapes. Update `_DUMP` to include all four tables:

```python
_DUMP = (
    'CREATE TABLE public."Voter" ("id" uuid NOT NULL, "State" text NOT NULL);\n'
    'CREATE TABLE public."DistrictVoter" ("district_id" uuid NOT NULL, "voter_id" uuid NOT NULL, "State" text NOT NULL);\n'
    'CREATE TABLE public."District" ("id" uuid NOT NULL, "state" text NOT NULL);\n'
    'CREATE TABLE public."DistrictStats" ("district_id" uuid NOT NULL, "buckets" jsonb);\n'
)


def test_creates_partitioned_and_flat(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = FakeConn()
    _patch(monkeypatch, conn)
    manifest = step.run(_CFG, "20260609")
    sql = executed_sql(conn)
    # Voter + DistrictVoter partitioned
    assert any('CREATE TABLE IF NOT EXISTS public."Voter" (' in s and 'PARTITION BY LIST ("State")' in s for s in sql)
    assert any('CREATE TABLE IF NOT EXISTS public."DistrictVoter" (' in s and 'PARTITION BY LIST ("State")' in s for s in sql)
    assert any('public."DistrictVoter_TX" PARTITION OF public."DistrictVoter" FOR VALUES IN (\'TX\')' in s for s in sql)
    # District + DistrictStats flat: plain create, NO partitioning
    assert any('CREATE TABLE IF NOT EXISTS public."District" (' in s for s in sql)
    assert any('CREATE TABLE IF NOT EXISTS public."DistrictStats" (' in s for s in sql)
    assert not any('public."District" ' in s and "PARTITION BY" in s for s in sql)
    # manifest lists every table + Voter/DistrictVoter children
    assert {"Voter", "DistrictVoter", "District", "DistrictStats"} <= set(manifest.tables_created)
    assert "Voter_TX" in manifest.tables_created and "DistrictVoter_TX" in manifest.tables_created


def test_build_partitioned_ddl_parametrizes_table_and_column() -> None:
    parent, children = step.build_partitioned_ddl(
        'CREATE TABLE public."DistrictVoter" ("voter_id" uuid NOT NULL, "State" text NOT NULL);',
        "DistrictVoter", "State", ["TX"],
    )
    assert 'CREATE TABLE IF NOT EXISTS public."DistrictVoter"' in parent
    assert parent.rstrip().endswith('PARTITION BY LIST ("State");')
    assert children == [
        'CREATE TABLE IF NOT EXISTS public."DistrictVoter_TX" PARTITION OF public."DistrictVoter" FOR VALUES IN (\'TX\');'
    ]
```

Keep `test_skips_when_complete`. The old single-table `test_applies_partitioned_table_and_extensions` and `test_build_partitioned_ddl_shape` should be updated: `build_partitioned_ddl` now takes a `partition_col` argument.

- [ ] **Step 2: Run — expect FAIL.** Run: `uv run pytest tests/test_create_schema.py -q`.

- [ ] **Step 3: Implement.** Replace `_TARGET_TABLE`, `build_partitioned_ddl`, and the table-handling in `run`:

```python
from loader.people_api.schema.schema_spec import TABLE_SPECS, is_partitioned, partition_column


def build_partitioned_ddl(
    create_sql: str, table: str, partition_col: str, states: Sequence[str]
) -> tuple[str, list[str]]:
    """Turn a plain CREATE TABLE into a LIST-partitioned parent + per-state children."""
    parent = create_sql.rstrip()
    if not parent.endswith(");"):
        raise RuntimeError("unexpected CREATE TABLE shape; cannot add PARTITION BY")
    parent = parent[:-1].rstrip() + f' PARTITION BY LIST ("{partition_col}");'
    parent = parent.replace(
        f'CREATE TABLE public."{table}"', f'CREATE TABLE IF NOT EXISTS public."{table}"', 1
    )
    children = [
        f'CREATE TABLE IF NOT EXISTS public."{table}_{s}" PARTITION OF public."{table}" '
        f"FOR VALUES IN ('{s}');"
        for s in states
    ]
    return parent, children


def _flat_ddl(create_sql: str, table: str) -> str:
    """Plain retry-safe CREATE for a non-partitioned table."""
    return create_sql.replace(
        f'CREATE TABLE public."{table}"', f'CREATE TABLE IF NOT EXISTS public."{table}"', 1
    )
```

In `run`, after `tables = extract_create_tables(schema_sql)`, replace the single-Voter block with iteration over `TABLE_SPECS` in a stable order, collecting statements and `tables_created`:

```python
    stmts: list[str] = []
    tables_created: list[str] = []
    for pg_table in TABLE_SPECS:  # dict preserves insertion order (Voter first) -> stable
        if pg_table not in tables:
            raise RuntimeError(
                f'target_schema.sql has no CREATE TABLE public."{pg_table}" (found: {sorted(tables)})'
            )
        create_sql = tables[pg_table]
        if is_partitioned(pg_table):
            col = partition_column(pg_table)
            assert col is not None  # is_partitioned guarantees it
            parent, child_stmts = build_partitioned_ddl(create_sql, pg_table, col, STATES)
            stmts.append(parent)
            stmts.extend(child_stmts)
            tables_created.append(pg_table)
            tables_created.extend(f"{pg_table}_{s}" for s in STATES)
        else:
            stmts.append(_flat_ddl(create_sql, pg_table))
            tables_created.append(pg_table)

    full_ddl = "\n".join(stmts)
    ddl_uri = put_artifact(cfg, run_date, "schema/target_schema.sql", full_ddl)
    log.info("schema.ddl_emitted", uri=ddl_uri, bytes=len(full_ddl))

    with connect_new(cfg, run_date) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE")
            cur.execute("CREATE EXTENSION IF NOT EXISTS aws_commons")
        for stmt in stmts:
            with conn.cursor() as cur:
                cur.execute(stmt)  # ty: ignore[no-matching-overload]
        log.info("schema.ddl_applied")
```

Delete `_TARGET_TABLE`. Update the module docstring's "the Voter table" to describe the four-table set.

> Note: parent partitioned tables must be created before their children — the loop appends parent then children per table, preserving that. Flat tables have no ordering constraint (no FKs).

- [ ] **Step 4: Run — expect PASS.** Run: `uv run pytest tests/test_create_schema.py -q`.

- [ ] **Step 5: Toolchain + commit.**

```bash
cd people-api-loader && uv run ruff check . && uv run ruff format --check . && uv run ty check
git add src/loader/people_api/steps/create_schema.py tests/test_create_schema.py
git commit -m "[DATA-2100] create_schema: create all four tables (partitioned + flat)"
```

---

## Task 4: Unload — per-table manifest + partitioned/flat unload + buckets transform

This is the largest task. It reshapes the unload manifest (a contract shared by copy + validate), so it also updates those two consumers' READS to the new shape while keeping their behavior Voter-only (their table-iteration is generalized in Tasks 5 and 7).

**Files:**
- Modify: `src/loader/people_api/manifests.py`, `src/loader/people_api/schema/unload_sql.py`, `src/loader/people_api/steps/unload.py`, `src/loader/people_api/steps/copy_s3.py` (manifest reads only), `src/loader/people_api/steps/validate.py` (manifest reads only)
- Test: `tests/test_unload_sql.py`, `tests/test_unload.py`, and fixture updates in `tests/test_copy_s3.py`, `tests/test_validate.py`, `tests/test_read_manifest.py` / `tests/test_copy_manifest.py` (any that build an `UnloadManifest`)

**Interfaces:**
- Produces: `UnloadTable` (per-table sub-record), `UnloadManifest.tables: list[UnloadTable]`, `UnloadFile.table: str`. S3 layout `{prefix}/{table}/state={s}/` (partitioned) and `{prefix}/{table}/data/` (flat), with the flat unit keyed by `state=""` in row counts / files.
- Consumes: `TABLE_SPECS`, `is_partitioned`, `unload_sql` builders.

### Sub-plan A — manifest reshape

- [ ] **Step A1: Failing manifest test** in `tests/test_read_manifest.py` (or a new focused test): construct an `UnloadManifest` with two `UnloadTable`s (a partitioned `Voter` and a flat `District`) and assert round-trip through the pydantic model, and that `UnloadFile` requires `table`.

- [ ] **Step A2: Implement** in `manifests.py`. Add `table` to `UnloadFile`; replace `UnloadManifest`'s per-table scalar fields with a list of sub-records:

```python
class UnloadFile(BaseModel):
    table: str
    state: str  # "" for a flat (non-partitioned) table's single unit
    s3_key: str
    size_bytes: int
    row_count: int


class UnloadTable(BaseModel):
    table: str
    databricks_table: str
    partition_by: str | None
    columns: list[str]
    column_types_pg: dict[str, str]
    # {state: n} for a partitioned table; {"": total} for a flat table.
    row_counts: dict[str, int] = Field(default_factory=dict)
    files: list[UnloadFile] = Field(default_factory=list)


class UnloadManifest(ManifestBase):
    step: Literal["unload"] = "unload"
    tables: list[UnloadTable]
```

(Confirm `UnloadFile`'s existing field name for the S3 key — the code uses `s3_key`. Keep it.)

- [ ] **Step A3:** Run the manifest test → PASS.

### Sub-plan B — unload_sql flat variants + transform hook

- [ ] **Step B1: Failing tests** in `tests/test_unload_sql.py`:

```python
from loader.people_api.schema import unload_sql


def test_flat_unload_has_no_state_where() -> None:
    sql = unload_sql.unload_statement_flat(
        mart_fqn="cat.s.m", select_exprs=["`a`", "`b`"], s3_dir="s3://x/District/data/"
    )
    assert "WHERE" not in sql
    assert "INSERT OVERWRITE DIRECTORY 's3://x/District/data/'" in sql


def test_flat_count_statement() -> None:
    assert unload_sql.count_all_statement("cat.s.m") == "SELECT count(*) AS n FROM cat.s.m"


def test_select_exprs_transform_renames_buckets() -> None:
    # DistrictStats: rename two struct fields inside buckets on the way out, to_json'd.
    exprs = unload_sql.select_exprs(
        ["district_id", "buckets"], extra_columns=set(),
        transforms={"buckets": unload_sql.BUCKETS_TO_JSON_EXPR},
    )
    assert exprs[0] == "`district_id`"
    assert "to_json" in exprs[1].lower()
    assert "presenceOfChildren" in exprs[1]
    assert "estimatedIncomeRange" in exprs[1]
    assert exprs[1].endswith("AS `buckets`")
```

- [ ] **Step B2: Implement** in `unload_sql.py`. Add a `transforms` param to `select_exprs`, a flat `unload_statement_flat`, `count_all_statement`, and the buckets rename expression. Keep the existing `select_exprs` signature working (extra param defaulted):

```python
def select_exprs(
    ddl_columns: list[str],
    extra_columns: set[str],
    transforms: dict[str, str] | None = None,
) -> list[str]:
    """Backtick-quoted SELECT expressions in DDL order.

    `transforms` maps a column name to a full SQL expression that REPLACES the plain
    `` `col` `` projection (the expression must alias itself `AS `col``); used for the
    DistrictStats buckets struct-field rename. Prisma-only extras become CAST(NULL AS STRING).
    """
    transforms = transforms or {}
    out: list[str] = []
    for col in ddl_columns:
        if col in transforms:
            out.append(transforms[col])
        elif col in extra_columns:
            out.append(f"CAST(NULL AS STRING) AS `{col}`")
        else:
            out.append(f"`{col}`")
    return out


def unload_statement_flat(*, mart_fqn: str, select_exprs: list[str], s3_dir: str) -> str:
    cols = ", ".join(select_exprs)
    return (
        f"INSERT OVERWRITE DIRECTORY '{s3_dir}'\n"
        f"USING csv OPTIONS ({_CSV_OPTIONS})\n"
        f"SELECT {cols}\n"
        f"FROM {mart_fqn}"
    )


def count_all_statement(mart_fqn: str) -> str:
    return f"SELECT count(*) AS n FROM {mart_fqn}"


# DistrictStats buckets: the active SQL mart emits two struct fields whose keys the app expects in
# camelCase. Rebuild the struct with those two fields renamed, then to_json for the jsonb column.
# The other three fields (age, homeowner, education) already match. This mirrors the legacy DAG's
# _BUCKET_KEY_MAP and is deleted once the camelCase `_py` districtstats mart is enabled.
BUCKETS_TO_JSON_EXPR = (
    "to_json(named_struct("
    "'age', `buckets`.`age`, "
    "'homeowner', `buckets`.`homeowner`, "
    "'education', `buckets`.`education`, "
    "'presenceOfChildren', `buckets`.`presence_of_children`, "
    "'estimatedIncomeRange', `buckets`.`estimated_income_range`"
    ")) AS `buckets`"
)
```

> Implementer MUST verify the exact source struct field names by reading `dbt/project/models/marts/people_api/m_people_api__districtstats.sql` (the final `struct(... ) as buckets` block, ~lines 421-427) and the three already-correct field names. The legacy remap keys (`airflow/astro/dags/load_people_api_old.py:45-48`) are lowercased-no-underscore, so the raw JSON key form may differ from the struct field identifier — build the `named_struct` off the actual struct field identifiers, and assert the output keys are the camelCase the app expects. Adjust the expression to the real field names; the test's `presenceOfChildren`/`estimatedIncomeRange` output-key assertions are the contract.

- [ ] **Step B3:** Run `uv run pytest tests/test_unload_sql.py -q` → PASS.

### Sub-plan C — unload.py multi-table

- [ ] **Step C1: Failing tests** in `tests/test_unload.py` (read the existing fixtures first). Cover: a partitioned table unloads per-state under `{prefix}/{table}/state={s}/`; a flat table unloads once under `{prefix}/{table}/data/` with no `WHERE State`; the manifest has one `UnloadTable` per `TABLE_SPECS` entry; DistrictStats uses the buckets transform.

- [ ] **Step C2: Implement** `unload.run` to iterate `TABLE_SPECS`. Per table: resolve `mart_fqn`, DDL columns/extras/types, choose `transforms` (`{"buckets": unload_sql.BUCKETS_TO_JSON_EXPR}` only for DistrictStats), then branch on `is_partitioned`:
  - Partitioned: loop `STATES`, `s3_dir=f"s3://{bucket}/{prefix}/{table}/state={s}/"`, `unload_sql.unload_statement(...)`, per-state count via `count_by_state_statement`, list files with `state=s`.
  - Flat: single `s3_dir=f"s3://{bucket}/{prefix}/{table}/data/"`, `unload_sql.unload_statement_flat(...)`, total count via `count_all_statement` stored under `row_counts={"": total}`, list files with `state=""`.
  Build one `UnloadTable` per table; assemble `UnloadManifest(tables=[...])`. Generalize `_list_state_files` to take a full prefix (`{prefix}/{table}/state={s}/` or `{prefix}/{table}/data/`) and a `table` + `state` to stamp on each `UnloadFile`. Preserve the `state_filter`/`skip_submit` partial-run guards (a `--state` run still only touches partitioned tables' matching state; document that flat tables are unloaded whole on a full run only). Delete `_TARGET_TABLE`.

  Key helper to add:

```python
def _table_prefix(prefix: str, table: str, state: str | None) -> str:
    """S3 subdir for a table's unit: partitioned -> <prefix>/<table>/state=<s>/, flat -> <prefix>/<table>/data/."""
    return f"{prefix}/{table}/state={state}/" if state is not None else f"{prefix}/{table}/data/"
```

- [ ] **Step C3:** Run `uv run pytest tests/test_unload.py -q` → PASS.

### Sub-plan D — keep copy + validate compiling against the new manifest

- [ ] **Step D1:** Update `copy_s3.py` and `validate.py` ONLY where they read the unload manifest, so they compile and their existing Voter tests pass with the new shape. Concretely, both currently read `unload.per_state_row_counts` and `unload.files`; add a small accessor and point them at the Voter `UnloadTable`:

In `copy_s3.py` `run`, replace `unload.per_state_row_counts` / `unload.files` usage by selecting the Voter table for now (behavior unchanged this task):

```python
    voter_unload = next((t for t in unload.tables if t.table == "Voter"), None)
    if voter_unload is None:
        raise RuntimeError("unload manifest has no Voter table")
    # (Task 5 generalizes this to iterate all unload.tables.)
```
and use `voter_unload.row_counts` / `voter_unload.files` in place of the old fields.

In `validate.py`, the unload-gate baseline currently reads `unload.per_state_row_counts`; point it at the Voter `UnloadTable.row_counts` (Task 7 generalizes).

- [ ] **Step D2:** Update every test fixture that builds an `UnloadManifest` (grep: `UnloadManifest(` and `UnloadFile(` across `tests/`) to the new `tables=[UnloadTable(...)]` shape. Run the FULL suite:

Run: `cd people-api-loader && uv run pytest -q`
Expected: PASS (all Voter behavior intact under the new manifest shape).

- [ ] **Step D3: Toolchain + commit.**

```bash
cd people-api-loader && uv run ruff check . && uv run ruff format --check . && uv run ty check
git add -A
git commit -m "[DATA-2100] unload: per-table manifest, flat + partitioned unload, buckets rename"
```

---

## Task 5: copy_s3 — iterate tables, per-(table,state) lock, flat path

**Files:**
- Modify: `src/loader/people_api/steps/copy_s3.py`
- Test: `tests/test_copy_s3.py`, `tests/test_copy_manifest.py`

**Interfaces:**
- Consumes: `UnloadManifest.tables`, `TABLE_SPECS`, `is_partitioned`, target-schema DDL per table.
- Produces: a `copy.run` that loads every unload table; `CopyTableResult.table` is the real table; flat tables load without a State dimension.

- [ ] **Step 1: Failing tests.** Add: (a) a flat table (District) loads its file(s) with a whole-table count/delete and `CopyTableResult(table="District", state="")`; (b) the advisory lock key differs for the same state across two tables (assert `_acquire_unit_lock` is called with a table-derived key); (c) Voter still loads per-state (regression). Read the existing `test_copy_s3.py` fixtures for the FakeConn + monkeypatch pattern.

- [ ] **Step 2: Implement.** Generalize:
  - `_copy_one_file` takes a `table` param (replace the hardcoded `f'public."{_TARGET_TABLE}"'`).
  - Replace `_acquire_state_lock(cur, state)` with `_acquire_unit_lock(cur, table, state)` mixing the table into the key so two tables' same-state loads don't collide:
    ```python
    def _acquire_unit_lock(cur: psycopg.Cursor, table: str, state: str) -> None:
        # hashtext is server-side/stable; combine table+state so per-table loads don't share a lock.
        cur.execute(
            "SELECT pg_advisory_lock(%s::int4, hashtext(%s))",
            (_COPY_LOCK_NAMESPACE, f"{table}:{state}"),
        )
    ```
  - `_count_state_rows`/`_delete_state` take `table` and, for flat tables (`state == ""`), operate on the whole table (`WHERE` omitted); for partitioned use `WHERE "State" = %s`.
  - Rename `_load_state` → `_load_unit(*, table, state, ...)`; for flat tables `state=""` means whole-table idempotency (count all / delete all / load all files).
  - `run` iterates `unload.tables`; per table derive its column list + force_null from that table's DDL (via `extract_create_tables(load_target_schema(...))[table]`), then load its units (states for partitioned, the single `""` unit for flat). Aggregate `results` across all tables. Completeness check becomes per-table: every table's expected units covered.
  - Delete `_TARGET_TABLE`.

  > `CopyTableResult` already has a `table` field — set it correctly per unit. The manifest's completeness gate should require all tables' expected units.

- [ ] **Step 3: Run — expect PASS.** `uv run pytest tests/test_copy_s3.py tests/test_copy_manifest.py -q`.

- [ ] **Step 4: Toolchain + commit.**

```bash
cd people-api-loader && uv run ruff check . && uv run ruff format --check . && uv run ty check
git add src/loader/people_api/steps/copy_s3.py tests/test_copy_s3.py tests/test_copy_manifest.py
git commit -m "[DATA-2100] copy_s3: load all tables, per-(table,state) lock, flat path"
```

---

## Task 6: build_indexes — iterate tables, partitioned + flat

**Files:**
- Modify: `src/loader/people_api/steps/build_indexes.py`
- Test: `tests/test_build_indexes.py`

**Interfaces:**
- Consumes: `TABLE_SPECS`, `is_partitioned`, `partition_column`, `primary_key_for`, `indexes_for`, `STATES`.
- Produces: a `build_indexes.run` that builds PK + indexes for every table — partitioned tables via the parent-only + per-partition-child + attach machinery with the partition key appended to PK/unique; flat tables via parent-level PK + plain indexes with no State rewrite. ANALYZE each.

- [ ] **Step 1: Failing tests.** Add: (a) flat-table PK is added WITHOUT a State column (`_add_primary_key` on District uses `("id")`, not `("id", "State")`); (b) a flat table's plain index is built directly on `public."District"` (no `ON ONLY` / child / attach); (c) `_plain_parent_only_sql`/`_plain_child_sql`/`_partition_sizes` parametrize the table name (DistrictVoter → `ON ONLY public."DistrictVoter"`, `Voter_%` → `DistrictVoter_%`); (d) DistrictVoter PK gets State appended → `(district_id, voter_id, State)`; (e) Voter behavior unchanged. Read the existing `test_build_indexes.py` for the `_order_children_largest_first`/`_partition_sizes` test style and the FakeConn pattern.

Representative new tests:

```python
def test_plain_parent_only_sql_parametrizes_table() -> None:
    idx = IndexDef(table="DistrictVoter", name="dv_idx",
                   sql='CREATE INDEX "dv_idx" ON public."DistrictVoter" USING btree ("district_id");',
                   unique=False, columns=["district_id"], where=None)
    out = step._plain_parent_only_sql(idx)
    assert 'ON ONLY public."DistrictVoter"' in out


def test_partition_sizes_uses_table_prefix(monkeypatch) -> None:
    # a fake conn returning DistrictVoter_TX / DistrictVoter_CA rows; assert prefix strip -> TX/CA
    ...


def test_flat_pk_has_no_state(monkeypatch) -> None:
    conn = FakeConn(); conn.queue_result(None)  # no existing PK
    step._add_primary_key(conn, PrimaryKey(table="District", constraint="District_pkey", columns=["id"]))
    assert any('ADD CONSTRAINT "District_pkey" PRIMARY KEY ("id")' in s for s in executed_sql(conn))
    assert not any('"State"' in s for s in executed_sql(conn))
```

- [ ] **Step 2: Implement.** Parametrize the partition literals and loop over tables:
  - `_plain_parent_only_sql(idx, table)`, `_plain_child_sql(idx, table, state)`, `_partition_sizes(cfg, run_date, table, *, forward)` — replace `_TARGET_TABLE` / `Voter_` with the `table` arg (`LIKE '<table>\_%'`, `.removeprefix(f"{table}_")`).
  - `_create_index(conn, idx, *, partition_key)` — append `partition_key` to the unique columns only when it is not None (partitioned); flat tables pass `partition_key=None` and the unique is built on its real columns. Keep the expression-column and empty-column guards.
  - `_analyze(cfg, run_date, table, *, forward)` — `ANALYZE public."<table>"`.
  - `run`: wrap the build work in a loop over `TABLE_SPECS`. For each table:
    - `pk = primary_key_for(table)`; append `partition_column(table)` to the PK columns ONLY if partitioned (dedup as today).
    - `idxs = indexes_for(table)`; split unique/plain.
    - Partitioned: `_add_primary_key`, `_create_index(..., partition_key="State")` for uniques, then parent-only + per-state children (largest-first via `_partition_sizes(..., table)`), then `_analyze(..., table)`.
    - Flat: `_add_primary_key` (no State), `_create_index(..., partition_key=None)` for uniques, plain indexes built directly on the table (a `_create_plain_flat(conn, idx)` that runs `_rewrite_index_sql(idx.sql)` unchanged), then `_analyze(..., table)`.
  - The `_ensure_instance_class` scale-up stays once at the top (it is table-agnostic).
  - `_l2type_coverage` stays Voter-only — call it once after the Voter table's build (it queries `table_name='Voter'` and prod `org_districts`); keep it outside the per-table loop, guarded to Voter.
  - Manifest: `indexes` aggregates all tables' `IndexSpec`s (State appended only where the source table is partitioned + unique); `constraints_added` across all PKs; `analyzed_tables` lists all four.

  > The cutover NOTE comment in `_create_index` (lines 137-145) stays and gains a sibling note: DistrictVoter's `(district_id, voter_id)` → `(district_id, voter_id, State)` is the same DATA-1855 divergence; the dbt write path's DistrictVoter `ON CONFLICT` must move to the composite key at cutover, not before.

- [ ] **Step 3: Run — expect PASS.** `uv run pytest tests/test_build_indexes.py -q`.

- [ ] **Step 4: Toolchain + commit.**

```bash
cd people-api-loader && uv run ruff check . && uv run ruff format --check . && uv run ty check
git add src/loader/people_api/steps/build_indexes.py tests/test_build_indexes.py
git commit -m "[DATA-2100] build_indexes: build all tables (partitioned + flat)"
```

---

## Task 7: validate — per-table gates

**Files:**
- Modify: `src/loader/people_api/steps/validate.py`
- Test: `tests/test_validate.py`

**Interfaces:**
- Consumes: `UnloadManifest.tables`, `InspectManifest.tables`, `TABLE_SPECS`, `is_partitioned`.
- Produces: a `validate.run` whose count gates cover every table — per-state (±10%) for partitioned tables, whole-table (±10%) for flat tables — plus per-table schema/index diffs.

- [ ] **Step 1: Failing tests.** Add: (a) a flat table (District) count gate compares a single total, not per-state; (b) the unload-integrity gate runs for every unload table; (c) DistrictVoter per-state gate runs; (d) Voter checks unchanged. Read the existing `test_validate.py` fixtures (it stubs `read_manifest` for inspect/unload and monkeypatches `connect_new`/`connect_prod`).

- [ ] **Step 2: Implement.** Generalize:
  - Rename `_new_voter_counts_by_state` → `_new_counts_by_state(cfg, run_date, table)` (parametrize the table in the `GROUP BY "State"` query); add `_new_total_count(cfg, run_date, table)` for flat tables.
  - Build the count-gate checks by iterating `unload.tables`: partitioned → `_compare_counts(f"row_counts_match_databricks:{table}", per_state, unload_table.row_counts)`; flat → a total-count ±10% check against `unload_table.row_counts[""]`.
  - `_check_prod_row_counts` iterates the inspect manifest's tables (already `list[TableInspection]`), matching each to its unload table; flat tables compare totals.
  - `_check_schema_diff` / `_check_indexes` parametrize the table (`_columns(conn, table)` from `information_schema`, `tablename=<table>` in `pg_indexes`); run per table and aggregate into per-table checks.
  - `_check_sample_queries` and `_check_l2type_coverage` stay Voter-only (the sample queries are Voter-column-specific; l2Type coverage is a Voter concern).
  - Keep the failed-manifest-on-exception guard and the skip-when-complete guard.

- [ ] **Step 3: Run — expect PASS.** `uv run pytest tests/test_validate.py -q`.

- [ ] **Step 4: Full suite + toolchain + commit.**

```bash
cd people-api-loader && uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run ty check
git add src/loader/people_api/steps/validate.py tests/test_validate.py
git commit -m "[DATA-2100] validate: per-table count/schema/index gates"
```

---

## Task 8: Docs + deployment runbook (no legacy deletion yet)

**Files:**
- Modify: `src/loader/people_api/CLAUDE.md` (pipeline-steps note now covers four tables), `docs/DATA-2100-district-family-design.md` (mark decisions as implemented), and the DAG doc `../../airflow/astro/docs/people_api_loader.md` if it states "Voter only".
- Create: a short "District family cutover runbook" section documenting the deployment-side steps.

The cutover-gated retirements (delete `airflow/astro/dags/load_people_api_old.py`; remove the dbt DistrictVoter write path; move the dbt `ON CONFLICT` to the composite keys) are **out of scope for this plan** and are listed here as the DATA-1855 follow-up only.

- [ ] **Step 1: Write the runbook** into the design doc's "Operational bootstrap" section — the exact order to run once the code lands on a deployment:
  1. Deploy the loader (`@main`/tag) so the CLI has the four specs.
  2. `loader emit-ddl` → commit the regenerated `schema/data/target_schema.sql` (now 4 CREATE TABLEs).
  3. `loader extract-serving-structure` against prod → commit the regenerated `schema/_serving_seed.py` (picks up real District/DistrictVoter indexes; confirms DistrictStats' live state). DistrictStats PK still comes from its spec.
  4. Trigger a `load_people_api` run on astro-dev; confirm all four tables green through `validate`.
- [ ] **Step 2: Update the pipeline-steps prose** in the two docs to say the loader builds Voter + the District family, and that Voter/DistrictVoter are partitioned by State while District/DistrictStats are flat.
- [ ] **Step 3: Commit.**

```bash
cd people-api-loader && git add -A
git commit -m "[DATA-2100] docs: four-table loader + deployment runbook"
```

---

## Operational prerequisites (deployment-side; NOT implementer tasks)

These need prod + Databricks credentials the implementers do not have locally (the `EngineerAccess` role is denied the prod DB connection string). They run on the deployment after the code lands and gate the first real end-to-end run — see Task 8's runbook:

1. `loader emit-ddl` → regenerate + commit `schema/data/target_schema.sql`.
2. `loader extract-serving-structure` → regenerate + commit `schema/_serving_seed.py` (also the live DistrictStats existence re-verify).

Until these run, `create_schema`/`unload`/`copy` still see only the committed Voter artifact — so the new-table code paths are exercised in unit tests (with fixtures) but not in a real run.

## Self-Review

- **Spec coverage:** design doc sections map to tasks — specs→T1, emit_ddl→T2, create_schema→T3, unload+manifest+buckets→T4, copy→T5, build_indexes→T6, validate→T7, retirement/runbook→T8. ✅
- **Placeholders:** the two intentionally research-gated spots (the exact `buckets` struct field names in T4-B2, and the flat-table test bodies) carry explicit "read this file, assert this contract" instructions with the contract pinned — not open TODOs. Implementers verify against the named source lines.
- **Type consistency:** `is_partitioned`/`partition_column`/`primary_key_for` (T1) are used with those exact names in T3/T6/T7; `UnloadTable`/`UnloadFile.table`/`row_counts` (T4) are consumed with those names in T5/T7; `_acquire_unit_lock`, `_load_unit`, `_plain_parent_only_sql(idx, table)` signatures are consistent between definition (T5/T6) and their tests.
