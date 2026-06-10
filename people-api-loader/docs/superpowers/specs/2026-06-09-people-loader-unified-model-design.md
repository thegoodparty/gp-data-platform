# People-API Loader — Unified Single-`Voter`-Table Design

**Status:** Approved (2026-06-09)
**Epic:** DATA-1640. Tickets: DATA-1910 (`create-schema`), DATA-1851 (`copy`), DATA-1853 (`build-indexes`), DATA-1911 (`validate`).
**Supersedes:** the 51-per-state-table approach in `docs/PLAN_LOADER.md`, which is stale.

## Background / why this exists

The first implementation pass built the four Postgres data-plane subcommands against `PLAN_LOADER.md`'s model: 51 standalone `public."Voter{ST}"` tables plus `VoterFile`. Capturing the real schema snapshot from the prod serving cluster (`gp-voter-db-20250728`), and confirming it against dev, showed prod actually uses a **unified single `public."Voter"` table**. That matches the omni `people-api` Prisma model and the DATA-1640 tickets. `PLAN_LOADER.md` is out of date. The first pass (PR #476) was closed; this design pivots the four steps to the real schema.

## Confirmed prod schema (ground truth)

From `pg_dump --schema-only` of prod `public`:

- **One table:** `public."Voter"`, ~354 columns, not partitioned. No `VoterFile`. No foreign keys.
- **Primary key:** `Voter_pkey PRIMARY KEY (id)`, where `id uuid NOT NULL` has **no default**.
- **`id` provenance:** a deterministic hash of `LALVOTERID`, computed upstream in the dbt people-api marts. Stable across refreshes, so consumers may reference `Voter.id`.
- **`LALVOTERID`:** `text NOT NULL` with a `UNIQUE INDEX "Voter_LALVOTERID_key"`.
- **`"State"`:** `text NOT NULL` — the per-state discriminator column (replaces per-state tables).
- **Infra columns:** `id uuid`, `created_at timestamp(3) DEFAULT CURRENT_TIMESTAMP NOT NULL`, `updated_at timestamp(3) NOT NULL` (no default).
- **Indexes:** 266 total (1 unique on `LALVOTERID` + 265 plain btree), all single-column or simple, **no partial (`WHERE`) indexes**. Names follow `Voter_<Col>_idx`.
- **Column naming:** mixed — some retain L2 prefixes (`Parties_Description`), others are shortened (`Active`, `Age`, `Airport_District`). The snapshot is the authoritative column list and naming.

## Design principle

The committed prod snapshot (`schema/data/prod_dump.sql`) is the single source of truth for both the table DDL and the index/constraint set. `create-schema` and `build-indexes` derive everything from it. We do **not** regenerate the schema from a curated Python column list. This retires `voter_columns.py`, the merge/type-inference logic in `emit_ddl.py`, `load_databricks_columns`, and `databricks_columns.json`.

## Step designs

### create-schema (DATA-1910)
1. Idempotency: read `schema` manifest; if `status=="complete"`, no-op.
2. Resolve the new-cluster writer endpoint via `resolve_writer_endpoint`.
3. Load the snapshot via `snapshot.load_prod_dump`. Extract only the `CREATE TABLE public."Voter"` statement(s) (pg_dump emits indexes/PK as separate `CREATE INDEX` / `ALTER TABLE ADD CONSTRAINT` statements, which are skipped here — indexes are deferred to build-indexes). A small helper parses the `CREATE TABLE` block(s) out of the dump.
4. Connect to the new cluster, `CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE` + `aws_commons`, apply the table DDL.
5. Write `SchemaManifest(status="complete", target_schema_s3_uri=<uploaded DDL>, tables_created=["Voter"], column_diff_from_prod={})`. Optional column verification against the snapshot's expected column set, logged not fatal.

No 51-table fan-out, no `VoterFile`, no Databricks column input.

### copy (DATA-1851)
Structure is unchanged from the prior design (group the unload manifest's files by state, `ThreadPoolExecutor`, one `aws_s3.table_import_from_s3` per file, largest-state-first), with these changes:
- Every import targets `'public."Voter"'` (the `"State"` column comes from the data).
- **Per-state idempotency keyed on the `"State"` column** (not table names): `SELECT count(*) FROM public."Voter" WHERE "State"=%s` vs the unload's `per_state_row_counts[state]`. Equal and `>0` → skip; `0 < actual < expected` → `DELETE FROM public."Voter" WHERE "State"=%s` then reload; `0` → load.
- Per-file error aggregation, then raise (unchanged).
- Manifest: `CopyManifest` written once at end; `status="complete"` only when every expected state is covered, else `"in_progress"` (the deliberate, documented exception for filtered/incremental runs). **`CopyTableResult` gains a `state: str` field** so per-state progress is tracked even though `table` is always `"Voter"`. Carry-forward of prior completed states keys on `state`.
- `--state` and `--parallelism` flags unchanged.

### build-indexes (DATA-1853)
1. Idempotency + resolver as above.
2. Load the snapshot; parse PK, unique, and indexes via the existing `index_specs` parser; filter to table `"Voter"`.
3. Apply in parallel (`ThreadPoolExecutor`): the PK (`id`) constraint plus all 266 indexes (the `LALVOTERID` unique + 265 plain btree). Concurrent `CREATE INDEX` against one table run in separate sessions (each takes a non-blocking lock); session tuning (`maintenance_work_mem`, `max_parallel_maintenance_workers`, timeouts) applied per worker. `CREATE INDEX IF NOT EXISTS` for idempotency. PK/unique via `ALTER TABLE ... ADD CONSTRAINT` / `CREATE UNIQUE INDEX`, swallowing `DuplicateObject`/`InvalidTableDefinition`.
4. `ANALYZE public."Voter"`.
5. `l2Type` coverage check against `"Voter"` (degrades to a warning if `org_districts` is unreachable — it lives in the app DB).
6. `IndexManifest(indexes=[...], constraints_added=[PK + unique names], analyzed_tables=["Voter"], l2type_coverage_missing=[...])`.

Dropped vs prior design: per-state filtering, round-robin interleave across tables, FK handling (none in the schema), and the `LEGACY_RENAMES` SQL rewrite (the snapshot already has correct column names). Kept: `IF NOT EXISTS` injection.

### validate (DATA-1911)
Five checks, single table, against the new cluster (and prod where noted):
1. `row_counts_match_databricks` — `SELECT "State", count(*) FROM public."Voter" GROUP BY "State"`, each state within **±10%** of the unload baseline (named constant). Non-empty mismatch set fails.
2. `schema_diff_clean` — `information_schema.columns` of new `"Voter"` vs prod `"Voter"`; new must be a superset (no missing prod columns; no unexpected columns beyond the snapshot's set).
3. `index_constraint_diff_clean` — `pg_indexes` on new `"Voter"` vs prod `"Voter"`; every prod index/constraint present on new.
4. `sample_queries_pass` — `voterFile.util.ts`-shaped queries against `public."Voter"` (party/gender/age-cast/district/family/JoP), return without error.
5. `l2Type_coverage` — every distinct `org_districts."l2Type"` maps to a column on `"Voter"`.
`all_passed=False` blocks handoff; CLI exits non-zero. Markdown report written via `put_artifact`. PK referenced is `id`.

## Components

**Reused as-is (carry over from the closed branch / already on main):**
- `resolve_writer_endpoint` in `people_api/db.py` (env `LOADER_NEW_WRITER_ENDPOINT` → provision manifest → error).
- `people_api/schema/index_specs.py` (pg_dump PK/index/FK parsers).
- `people_api/schema/snapshot.py` — `load_prod_dump` only (drop `load_databricks_columns`).
- `tests/_fakes.py` (FakeConn recorder).
- `core/*`, `cli.py`, `manifests.py`, the `.pre-commit-config.yaml` sqlfmt exclude for `schema/data/`.

**New:**
- `schema/data/prod_dump.sql` — the real captured prod snapshot (already captured; lives at `/tmp/voter_prod_dump_real.sql`, to be committed). No `databricks_columns.json`.
- A "extract `CREATE TABLE` statements from a pg_dump" helper (small function; either a trimmed `emit_ddl.py` or folded into create-schema).
- The four step implementations + their tests.
- `CopyTableResult.state: str` field in `manifests.py`.

**Retired (not carried over):** `voter_columns.py`, the merge/emit logic of `emit_ddl.py`, `load_databricks_columns`, `databricks_columns.json`.

## Dependencies & assumptions (document in PR)

- **Unload export contract (DATA-1908, out of scope):** the S3 CSVs must include `id` (hash of `LALVOTERID`), `"State"`, and `updated_at`, with **column order matching the `Voter` table**, because `aws_s3.table_import_from_s3` is called with an empty column list (full-row import in table order).
- `provision`/`inspect-prod` remain out of scope; decoupled via `resolve_writer_endpoint` and the committed snapshot.
- `copy`/`validate` consume the `unload` manifest as a read contract (stubbed in tests, required at runtime).

## Testing approach

Unit tests mock DB connections via `tests/_fakes.py` (FakeConn records executed SQL, serves queued results) and monkeypatch `connect_new`/`connect_prod`/`read_manifest`/`write_manifest`/`put_artifact`/`resolve_writer_endpoint` on each step module. Per step, assert on the real SQL emitted and the returned manifest:
- create-schema: extracts only `CREATE TABLE "Voter"` (no `CREATE INDEX`), installs extensions, `tables_created==["Voter"]`.
- copy: targets `public."Voter"`; per-state count/delete/reload branches; `state` recorded in results; completion logic.
- build-indexes: PK + unique + indexes applied to `"Voter"`, `ANALYZE "Voter"`; `IF NOT EXISTS` injection.
- validate: ±10% per-state tolerance incl. `expected=0` edge; aggregation; markdown emitted.
The `index_specs` parser and the `CREATE TABLE` extraction helper are pure and tested directly with fixture dump strings. Full suite green under `uv run pytest`; `ruff`/`ty` clean (note: RUF100 forbids unused `# noqa` — use plain comments for intentional broad `except`; dynamic-SQL `cur.execute` needs `# ty: ignore[no-matching-overload]`).

## Out of scope

`inspect-prod` (1907), `unload` (1908), `provision` (1909), `resize` (1854), `status`/`teardown` (1912); the Airflow DAG; production cutover.
