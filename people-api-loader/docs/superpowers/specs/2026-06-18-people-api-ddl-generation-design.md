# People-API target-schema generation design

**Status:** approved design, pre-implementation
**Date:** 2026-06-18
**Relates to:** DATA-1640 epic, DATA-1904 (DDL from source), DATA-1907 (inspect-prod), DATA-1910 (create-schema)
**Supersedes:** the committed `schema/data/prod_dump.sql` as the loader's schema source of truth
**Delivery:** both phases ship in one implementation plan, on PR #504 (branch
`feat/DATA-1909-cluster-lifecycle`).

## Problem

The people-api-loader builds a fresh Aurora cluster per refresh and needs the target
Postgres schema for the people-api tables. Today that schema comes from a hand-produced
`schema/data/prod_dump.sql` (a `pg_dump --schema-only` snapshot of the serving cluster),
read by two steps:

- `create_schema` (step 3): extracts `CREATE TABLE public."Voter"` and partitions it by state.
- `build_indexes` (step 5): parses the dump's `ALTER TABLE ... ADD CONSTRAINT PRIMARY KEY`,
  `CREATE INDEX`, and FK statements (`schema/index_specs.py`) and re-issues them.

A static dump drifts from reality: when a dbt mart adds or retypes a column, the dump is
stale until someone re-runs `pg_dump` by hand. We want the schema to be **generated** from
the people-api mart tables so it cannot silently diverge from the data being loaded.

## Key insight: two kinds of schema, two owners

The schema splits into two concerns with different sources of truth:

| Concern | Source of truth | Why |
|---|---|---|
| Columns + types | The dbt **marts** (Databricks introspection) | Data-shaped. Introspecting the materialized mart guarantees the Postgres columns/types match exactly what the unload produces — no drift possible. |
| PK, indexes, partitions, table configs | A version-controlled **schema spec** in the loader repo | Serving-shaped. Databricks/Delta has no concept of btree/partial indexes, partitioning, or storage configs. These exist nowhere today except the prod cluster (inside `prod_dump.sql`). |

"Define the schema" = render those two sources into one artifact. The spec is seeded once
from the live prod cluster (the only place the serving structure currently lives), then
maintained in code.

## Decisions (settled during brainstorming)

1. **Column/type source:** introspect the materialized Databricks marts (`DESCRIBE` /
   `databricks tables get`), not the dbt contract yaml and not the prod dump.
2. **Scope:** Voter-only. (Revised from "all four marts" after the bootstrap revealed:
   `DistrictStats` is not a serving Postgres table; `District`/`DistrictVoter` are small,
   Prisma-defined — uuid PKs, timestamps, a `state` enum the marts don't describe — and are
   built by the dbt write path, not bulk-loaded by this loader. Voter is the table the loader
   loads and where mart-vs-dump drift actually matters. Empirical check: the Voter mart has 353
   columns; the serving table has 354 — the only omitted column is `Mailing_HHGender_Description`
   (a REMOVED_COLUMNS NULL placeholder), declared as a `TableSpec.extra_columns` Prisma-layer
   entry. Generated DDL has exact column parity with the Voter-only `prod_dump.sql`.)
3. **Generation timing:** committed artifact produced by a `loader emit-ddl` CLI step
   (reviewable git diffs, deterministic reruns, loader run needs no Databricks access),
   not live introspection inside `create_schema`.
4. **Index source:** bootstrapped from the live prod cluster via `inspect-prod`
   (`pg_catalog`), then codified into the schema spec. This retires `prod_dump.sql` entirely.
5. **Spec form:** a declarative Python spec (`schema_spec.py`) that reuses the existing
   `PrimaryKey` / `IndexDef` / `ForeignKey` dataclasses, not a Jinja SQL template.

## Architecture

Six units, each independently understandable and testable.

### 1. Mart introspection — `schema/mart_introspect.py`
`introspect_mart(cfg, mart_fqn) -> list[Column]` where `Column = (name, spark_type, nullable)`.
Reads the materialized mart schema from Databricks (`databricks tables get
<catalog>.<schema>.<model>`, or `DESCRIBE TABLE`). Pure read; no PG knowledge.
- Inputs: the four mart fully-qualified names, resolved from config
  (`goodparty_data_catalog.<schema>.m_people_api__{voter,district,districtstats,districtvoter}` —
  the exact dbt schema is confirmed during implementation).

### 2. Type mapping — `schema/type_map.py`
`to_pg_type(spark_type) -> str`. Deterministic Spark/Delta → Postgres map:
`string→TEXT`, `int→INTEGER`, `bigint→BIGINT`, `smallint→SMALLINT`, `double→DOUBLE PRECISION`,
`float→REAL`, `boolean→BOOLEAN`, `date→DATE`, `timestamp→TIMESTAMPTZ`,
`decimal(p,s)→NUMERIC(p,s)`, `binary→BYTEA`. Unknown types raise (fail loud, never guess).
Because we introspect the **mart** (not the raw `int__l2_nationwide_uniform`), the
people-api-shaped casts already hold — e.g. `Age` is `string→TEXT` and `Age_Int` is
`int→INTEGER`, matching `m_people_api__voter`'s `cast`/`try_cast`.
Per-column **type overrides** in the schema spec handle cases where the PG type differs from
the naive map — notably `id`, which the mart emits as a salted-uuid `string` but the contract
declares `uuid` (→ `UUID` in PG).

### 3. Serving-structure extraction — extend `inspect-prod` (step 0)
Adds a `pg_catalog` read against the serving cluster (already connected via `connect_prod`):
- Indexes: `pg_get_indexdef` over `pg_indexes` (captures the ~266 Voter indexes verbatim,
  including partial-index `WHERE` clauses).
- PK / FK: `pg_get_constraintdef` over `pg_constraint`.
- Partition scheme: `pg_partitioned_table` / `pg_get_partkeydef`.
Emits these as `PrimaryKey` / `IndexDef` / `ForeignKey` records, written as a per-run S3
artifact and a human-readable form used to seed (§4). This is the one-time bootstrap.

### 4. Declarative schema spec — `schema/schema_spec.py`
Per-table entries describing the serving structure the marts cannot know:
```
TABLE_SPECS = {
    "Voter": TableSpec(
        mart_fqn=...,                       # which mart supplies columns
        pg_table='public."Voter"',
        partition=PartitionByList(column="State", values=STATES),
        primary_key=PrimaryKey(...),         # seeded from §3
        indexes=[IndexDef(...), ...],        # ~266, seeded verbatim from §3
        foreign_keys=[...],
        type_overrides={"id": "UUID"},
        configs={...},                       # storage params if any
    ),
    "District": TableSpec(... partition=None, ...),   # plain table
    "DistrictStats": TableSpec(...),
    "DistrictVoter": TableSpec(...),
}
```
Reuses the existing `index_specs` dataclasses so `build_indexes` needs no new vocabulary.
For the three small tables, PK/unique and FK relationships can come from the mart contract
(`m_people_api.yaml` declares `data_type`, `not_null`, `unique`, `relationships`); Voter's
indexes come from the prod bootstrap. The exact PG table names for the non-Voter tables are
confirmed against `write__people_api_db.py` / the app during implementation.

### 5. Renderer + CLI — `schema/emit_ddl.py`, `loader emit-ddl`
For each table: introspect mart columns (§1) → map types (§2) + apply spec overrides →
combine with the spec's partition strategy → render `CREATE TABLE` statements → write the
committed `schema/data/target_schema.sql`. Deterministic: same marts + same spec → byte-identical
output, so a real schema change is a reviewable diff. `target_schema.sql` holds table DDL only;
PK/indexes/FKs stay as spec records (consumed by `build_indexes`), mirroring today's split.

### 6. Consumers — `create_schema` and `build_indexes`
- `create_schema`: `load_target_schema()` reads the committed `target_schema.sql` (replacing
  `load_prod_dump` + `extract_create_tables`) and applies all four tables. Voter keeps the
  LIST-by-state parent + per-state child partitions, now driven by the spec's partition strategy.
- `build_indexes`: reads PK/index/FK records directly from `schema_spec.py` (replacing the
  `load_prod_dump` + `index_specs` parse — no SQL parsing needed anymore).
- `schema/data/prod_dump.sql`, `schema/snapshot.py:load_prod_dump`, and the parsing path in
  `index_specs.py` are removed.

## Data flow

```
dbt marts (Databricks) ──introspect──┐
                                      ├─ emit-ddl ─> target_schema.sql (committed)
schema_spec.py (partitions/configs) ──┘                    │
prod cluster ──inspect (pg_catalog)──> schema_spec.py      │
   (one-time bootstrap; codified)         │                │
                                          │   create_schema reads target_schema.sql
                          build_indexes reads schema_spec records
```

## Phasing

**Phase 1 — generate and validate in parallel (additive, no behavior change).**
Build §1, §2, §3, §4, §5. `emit-ddl` produces `target_schema.sql` and the bootstrap captures
the serving structure into `schema_spec.py`. The live steps still read `prod_dump.sql`, so
nothing changes at run time. The payoff: we **diff the generated table DDL against the prod
dump** to prove parity (and surface intended differences) before depending on it. Low risk,
independently useful.

**Phase 2 — cut over and retire the dump.** Point `create_schema` at `target_schema.sql` and
`build_indexes` at `schema_spec.py`; delete `prod_dump.sql`, `load_prod_dump`, and the
`index_specs` parser. Safe because Phase 1 validated parity.

## Error handling

- Type map: unknown Spark type → raise (never guess a PG type).
- Mart introspection: a mart missing / unreachable → raise with the mart FQN.
- Renderer: a spec table whose mart columns don't include a spec PK/index column → raise
  (catches a column rename that would otherwise emit a broken index).
- `emit-ddl` is idempotent and offline-of-the-loader-run: re-running with no upstream change
  is a no-op diff.

## Testing

- `type_map`: table-driven unit tests incl. `decimal(p,s)` and the unknown-type raise.
- `mart_introspect`: unit test against a fake Databricks response; the real call is covered by
  one integration test gated like the existing `test_integration_pg.py`.
- `emit_ddl` renderer: golden-file test — fixed fake mart columns + a fixed spec render a known
  `target_schema.sql` (asserts partition wrapping, type mapping, overrides).
- `inspect-prod` extraction: unit test parsing fake `pg_catalog` rows into the dataclasses.
- Phase-1 parity: a one-off check (not a permanent test) diffing generated DDL vs the current
  `prod_dump.sql`, with intended differences enumerated.

## Non-goals

- Changing the **unload** source (DATA-1908). This design assumes the copy step loads columns
  matching the marts; aligning the unload to the marts is tracked separately. (Flag: if the
  unload still reads `int__l2_nationwide_uniform` with L2-native names, the generated mart-shaped
  DDL won't match — the unload must read the marts. Called out for the plan.)
- Migrations / online schema change. This generates the schema for a fresh cluster only.
- Replacing `write__people_api_db.py` (the existing dbt write path).

## Open items to confirm during implementation

- The dbt schema the marts materialize into (`goodparty_data_catalog.dbt.*` vs a `marts` schema).
- The exact PG table names for `district`, `districtstats`, `districtvoter` (from
  `write__people_api_db.py` / the app's Prisma).
- Partition/PK strategy for the three non-Voter tables (likely plain tables with a PK from the
  mart contract; confirm none are large enough to warrant partitioning).
