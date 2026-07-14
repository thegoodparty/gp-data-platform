# DATA-2100: load the District family (District, DistrictStats, DistrictVoter)

Design and operational reference for extending the People API loader from a Voter-only
bulk load to the full serving set: `District`, `DistrictStats`, and `DistrictVoter`
alongside `Voter`, all built into the same freshly-provisioned Aurora cluster each refresh.

Companion to the DAG TDD at `../../airflow/astro/docs/people_api_loader.md` (DATA-1735 /
DATA-1913). This document covers the loader-package changes only; the DAG sequence itself
is unchanged (the same `loader <step>` BashOperators now operate on four tables instead of one).

## Goal

Retire the legacy `load_people_api_old` DAG and the dbt write path's DistrictVoter loading by
having the train-deployment loader build the entire serving table set on the fresh cluster, so a
single refresh produces a complete, swap-ready database.

## Current state (why this is mostly generalization, not new machinery)

The pipeline's substrate is already table-generic — `emit_ddl.render_target_schema` iterates
`TABLE_SPECS`, `serving_structure.extract_*` take a table list, `index_specs` dataclasses are
table-scoped, and `config._MART_MODELS` already maps all four tables to their marts. What makes the
loader Voter-only today is:

1. `schema/schema_spec.py::TABLE_SPECS` contains a single entry (`"Voter"`).
2. Every pipeline step hardcodes `_TARGET_TABLE = "Voter"` and the literals `"State"` /
   `Voter_<state>`, and the declarative `TableSpec.partition_by` field is dead metadata that no step
   reads.
3. Two committed, generated artifacts describe only Voter: `schema/data/target_schema.sql`
   (one `CREATE TABLE`) and `schema/_serving_seed.py` (indexes for Voter only; PKs for
   District/DistrictVoter/Voter; no DistrictStats; `FOREIGN_KEYS = []`).

So the work is: grow `TABLE_SPECS` to four entries, make `partition_by` live, replace each Voter
literal with iteration over the specs keyed on the partitioned-vs-flat distinction, and regenerate
the two artifacts.

## Table shapes (established from Prisma, the marts, and the serving snapshot)

| Table | Grain | Partition (fresh cluster) | PK | Notes |
|---|---|---|---|---|
| `Voter` | one row / voter | LIST by `State` | `(id, State)` | unchanged |
| `DistrictVoter` | one row / (voter, district) | **LIST by `State`** | `(district_id, voter_id, State)` | large membership table; mart has `state`; serving PK is flat `(district_id, voter_id)` — see cutover |
| `District` | one row / district | flat | `(id)` | small; mart filters `state != 'US'` in legacy path |
| `DistrictStats` | one row / district | flat | `(district_id)` | `buckets` jsonb; **not in serving today**; PK defined in spec, not seed |

The serving cluster enforces **no foreign keys** (`FOREIGN_KEYS = []`), so the loader creates none —
this removes the parent-before-child ordering that would otherwise couple District → DistrictVoter.

## Design decisions

### 1. Partitioned-vs-flat is driven by `TableSpec.partition_by`

Make the existing field live. `partition_by="State"` → LIST-partitioned with per-state children;
`partition_by=None` → plain flat table. This single flag drives create_schema, build_indexes,
unload, copy, and validate, replacing the hardcoded `_TARGET_TABLE`/`"State"`/`Voter_<state>`
literals. DistrictVoter and Voter are partitioned; District and DistrictStats are flat.

### 2. DistrictVoter is partitioned by State

It is a voter×district membership table (multiple district rows per voter), so it is large enough to
need the same parallel per-partition index build and per-state copy that Voter uses. Partitioning
requires the partition key in the PK, so the loader's PK becomes `(district_id, voter_id, State)` —
diverging from serving's `(district_id, voter_id)`. This is the same divergence already planned for
Voter's composite unique at cutover (DATA-1855): the dbt write models' `ON CONFLICT` for DistrictVoter
must move to the State-inclusive key at cutover, not before.

### 3. DistrictStats structure lives in the spec, not the serving seed

DistrictStats is not a serving Postgres table today, so `extract-serving-structure` cannot capture
its PK, and any hand-edit to the generated `_serving_seed.py` would be wiped on the next regen.
Its PK `(district_id)` is therefore declared explicitly on its `TableSpec`, and the
`primary_key_for` lookup falls back to the spec's declared PK when the serving seed has no entry for
the table. DistrictStats has no secondary indexes.

### 4. DistrictStats `buckets` key rename happens in the unload SQL

The active `m_people_api__districtstats` mart is the SQL model, which emits the `buckets` struct with
non-camelCase keys for two dimensions (`presence_of_children`, `estimated_income_range`); the app
expects camelCase, which is why the legacy DAG applies `_BUCKET_KEY_MAP`. A camelCase-correct Python
mart (`m_people_api__districtstats_py.py`) exists but is `enabled=False`.

To keep DATA-2100 self-contained (not blocked on a dbt PR to flip the mart), the loader renames those
two struct fields in its unload SELECT for DistrictStats, before serializing `buckets` to JSON. This
is a per-table projection override — a new, small hook keyed by table on the unload path
(`unload_sql.select_exprs` currently has no per-table transform). When the `.py` mart is later
enabled, the override is deleted and the loader copies `buckets` straight.

### 5. `buckets` is stored as `jsonb`

A `type_overrides={"buckets": "jsonb"}` on the DistrictStats spec maps the mart's struct type to a
Postgres `jsonb` column; the unload emits `to_json(buckets)` (with the rename from decision 4) and the
`aws_s3` import loads the JSON text.

## Per-step changes

- **`schema/schema_spec.py`** — add `District`, `DistrictStats`, `DistrictVoter` to `TABLE_SPECS`
  (`DistrictVoter.partition_by="State"`; the two flat tables `None`; `DistrictStats` carries an
  explicit PK + `buckets` jsonb override). Rewrite the scope comment. Add an optional `primary_key`
  field to `TableSpec` and make `primary_key_for` fall back to it when the seed lacks the table.
- **`schema/emit_ddl.py`** — no logic change (already iterates `TABLE_SPECS`); the committed
  `target_schema.sql` is regenerated via `loader emit-ddl`.
- **`steps/create_schema.py`** — iterate the parsed DDL / specs; generalize `build_partitioned_ddl`
  to take a table name + partition column and drive child names off the table; apply flat CREATE for
  `partition_by=None`; extend `tables_created`.
- **`steps/build_indexes.py`** — loop over target tables; for partitioned tables generalize
  `_TARGET_TABLE`/`Voter_<state>`/`_partition_sizes` (`LIKE 'Voter\_%'`, prefix-strip) and the
  State-append on PK/unique to the table name; for flat tables build PK + indexes at parent level with
  no State rewrite; ANALYZE each table. Keep largest-first scheduling across all partitioned units.
- **`steps/unload.py` + `schema/unload_sql.py`** — iterate specs; partitioned tables keep the
  per-state `WHERE "State"=` loop and `state=<s>/` prefix; flat tables do a single unpartitioned
  unload under a flat prefix. Add a per-table `select_exprs` transform hook (used by DistrictStats'
  buckets rename). Key the unload manifest per table.
- **`steps/copy_s3.py`** — iterate specs; **add the table into the per-state advisory-lock key** so
  two tables' same-state loads don't collide; parametrize the target table + column list; flat tables
  COPY without a State dimension (whole-table count/delete for idempotency).
- **`steps/validate.py`** — per-state count gates for partitioned tables (Voter, DistrictVoter);
  whole-table count gates for flat tables (District, DistrictStats); per-table schema/index diffs
  against the regenerated seed.
- **`config.py`** — no wiring change (`_MART_MODELS` already maps all four). Revisit
  `_DEFAULT_BUILDERS` / instance classes only if the added tables shift the CPU/IO profile.

## Operational bootstrap (deployment-side; blocks the code changes at runtime)

Two committed artifacts are the practical gate on everything downstream, and both require the
deployment's identity (the local `EngineerAccess` role is explicitly denied the prod DB connection
string; Databricks mart introspection also needs the deployment credentials):

1. **`schema/data/target_schema.sql`** — regenerate with `loader emit-ddl` once `TABLE_SPECS` has all
   four entries. Needs Databricks access (introspects each mart). Commit the result.
2. **`schema/_serving_seed.py`** — regenerate with `loader extract-serving-structure` against the prod
   serving cluster (already extracts all four tables). Picks up any real District/DistrictVoter
   indexes and confirms the live DistrictStats state. Commit the result. DistrictStats' PK still comes
   from its spec (decision 3), since it is absent from serving.

Running `extract-serving-structure` also serves as the **live DistrictStats existence re-verify** the
serving snapshot could not settle locally.

## Legacy retirement (after the new path validates end-to-end)

Once a full `load_people_api` run builds and validates all four tables:

- Delete `airflow/astro/dags/load_people_api_old.py` (its `load_districts` + `load_district_stats`
  tasks are now covered by the loader).
- Remove DistrictVoter loading from the dbt write path (`write__people_api_db.py`), coordinated with
  the DATA-1855 cutover (the `ON CONFLICT` key change).

Until then, `load_people_api_old` keeps District/DistrictStats fresh on the serving cluster and is not
touched.

## Testing

- Unit tests per generalized step: partitioned-vs-flat branch selection driven by `partition_by`;
  flat-table create/copy/validate paths; the per-table advisory-lock key; the DistrictStats buckets
  rename transform; `primary_key_for` spec fallback.
- The existing Voter tests must stay green unchanged — the generalization must be behavior-preserving
  for Voter (a four-entry `TABLE_SPECS` iterated in a stable order must reproduce today's Voter-only
  SQL for the Voter table).
- End-to-end validation is a full `load_people_api` run on astro-dev with all four tables green
  through `validate`.

## Risks / open items

- **DistrictVoter scale is unconfirmed.** Partitioning is the safe choice for a large table; if a
  deployment-side row count shows it is small, flat would have been simpler, but partitioning is not
  wrong for a small table (just slightly more objects).
- **Cutover coupling (DATA-1855).** The DistrictVoter composite-PK divergence and the dbt `ON CONFLICT`
  change must land together at cutover, not piecemeal.
- **Instance sizing.** Adding DistrictVoter (potentially larger than Voter) to the index build may
  change the `db.r8g.48xlarge` / `_DEFAULT_BUILDERS=128` assumptions; watch the first full run.
