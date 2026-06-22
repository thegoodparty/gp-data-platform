# Unload (Databricks → S3) design — DATA-1908

**Status:** approved design, pre-implementation
**Date:** 2026-06-22
**Relates to:** DATA-1640 epic, DATA-1908 (unload), DATA-1851 (copy — consumes the output), DATA-1904 (DDL — defines the column contract), DATA-1905 (S3 bucket + UC external location — write target)
**Delivery:** PR on branch `feat/DATA-1908-unload`.

## Problem

The loader's `unload` step is the one remaining unimplemented CLI subcommand (stub at
`steps/unload.py`). It exports the people-api voter data from Databricks to S3 in the format
the already-shipped `copy` step imports, so the end-to-end pipeline
(unload → provision → create-schema → copy → build-indexes → resize → validate) can run.

## Decisions (settled during brainstorming)

1. **Source:** the dbt mart `m_people_api__voter` (not `int__l2_nationwide_uniform`). Consistent
   with DATA-1904: the mart already does the L2→PG renames, the `try_cast` derived columns
   (`Age_Int`, …), and the column projection, so unload columns line up with `target_schema.sql` /
   create-schema / copy by construction — no separate column-mapping layer.
2. **Mechanism:** Databricks SQL via `databricks-sdk` statement-execution against a SQL warehouse
   (`LOADER_DATABRICKS_WAREHOUSE_ID`). `loader unload` is a thin orchestrator (build SQL → submit →
   poll → record manifest), matching the rest of the loader. No PySpark job artifact, no cluster
   lifecycle.
3. **Wire format:** CSV on both sides. Unload writes Spark CSV (native quoting/escaping for embedded
   tab/newline/quote/backslash); a small follow-up edits `copy_s3.py`'s `aws_s3.table_import_from_s3`
   options from `FORMAT text` to `FORMAT csv`. Robust to dirty free-text across 218M rows.
4. **Per-state files:** one `INSERT OVERWRITE DIRECTORY` per state into a `state=XX/` prefix, so files
   are state-tagged for `copy` (which groups `files[]` by state) and `--state FL` / the FL-first
   milestone work. Big states fan out to multiple part files (good for `copy`'s per-file parallelism).

## Architecture

`loader unload --date X [--state XX] [--skip-submit]` →
`steps/unload.py:run(cfg, run_date, *, state_filter=None, skip_submit=False) -> UnloadManifest`.

Flow:
1. Manifest skip-guard: a complete `unload` manifest short-circuits (unless `--state` is given).
2. Build the column contract from `target_schema.sql` (via `load_target_schema` + `extract_column_names`)
   and `schema_spec` (`extra_columns`). The ordered `SELECT` expression list, for each DDL column:
   - a declared Prisma-extra (e.g. `Mailing_HHGender_Description`) → `NULL AS "<col>"`;
   - otherwise → `` `<col>` `` (the same-named mart column).
   This yields all 354 DDL-ordered columns, so file column order == `copy`'s column list exactly.
3. For each state (all 51, or just `state_filter`): submit
   `INSERT OVERWRITE DIRECTORY 's3://{bucket}/{export_prefix}/state=XX/' USING csv OPTIONS (...) SELECT <exprs> FROM {mart_fqn} WHERE State = 'XX'`
   via the SDK; poll to a terminal state.
   - `--skip-submit`: build + log the SQL, do not execute (dry-run/inspection).
4. `per_state_row_counts`: `SELECT State, count(*) FROM {mart_fqn} GROUP BY State` (one cheap query).
5. `files[]`: list S3 (boto3) under each `state=XX/` prefix → `UnloadFile{state, s3_key, size_bytes, row_count}`.
6. Write `UnloadManifest{databricks_table=mart_fqn, columns, column_types_pg, per_state_row_counts, files, status=complete}`.

## New / modified units

- **`schema/unload_sql.py`** (new, pure/testable): builds the per-state `INSERT OVERWRITE DIRECTORY`
  SQL and the count SQL from the DDL column contract + `schema_spec`. No I/O.
- **`core/databricks.py`** (new): thin `databricks-sdk` helpers — a cached `WorkspaceClient`, and
  `execute_sql(cfg, statement) -> result` that submits to the warehouse and polls to terminal
  (raising on FAILED/CANCELED). Mirrors `core/aws.py`'s thin-client style. (`mart_introspect` from
  #519 may move its lazy `WorkspaceClient` here; keep its public API unchanged.)
- **`steps/unload.py`**: the `run()` orchestration above (replaces the stub).
- **`config.py`**: add `databricks_warehouse_id` (`LOADER_DATABRICKS_WAREHOUSE_ID`); `mart_fqns`
  already added in #519.
- **`steps/copy_s3.py`**: change the import options string `FORMAT text, DELIMITER E'\t', NULL '\N'`
  → `FORMAT csv, NULL '', QUOTE '"', ESCAPE '"'` (paired exactly with the unload OPTIONS). One-line
  contract edit; update the `copy` tests' expected options.
- **CSV/COPY option pairing (pinned):** unload `OPTIONS('sep'='\t','header'='false','nullValue'='','quote'='"','escape'='"')`
  ↔ copy `(FORMAT csv, DELIMITER E'\t', NULL '', QUOTE '"', ESCAPE '"', ENCODING 'UTF8')`. Tab delimiter
  keeps commas-in-data quote-free; `nullValue=''`+`NULL ''` round-trips NULLs; quote/escape both `"`.

## Manifest semantics

`columns` (354 DDL-ordered names) and `column_types_pg` (parsed from `target_schema.sql`, the
authoritative PG types) describe the file layout. `per_state_row_counts` is authoritative and is what
`validate` diffs the loaded cluster against. `files[]` gives `copy` the `s3_key`s (grouped by state)
and `size_bytes` (zero-size files are skipped). `UnloadFile.row_count` is best-effort: `INSERT OVERWRITE
DIRECTORY` reports a per-*state* count, not per-*file*, and nothing consumes per-file counts, so it
carries the per-state total on the first file and 0 on the rest. (A conscious choice, not an oversight.)

## Error handling

- A statement that polls to FAILED/CANCELED raises with the Databricks error text; never write a
  `complete` manifest on partial output.
- A zero-row state writes an empty `state=XX/` dir and a 0 count — not an error (copy skips it).
- Missing/empty `databricks_warehouse_id` or `mart_fqns["Voter"]` → raise an actionable config error
  before submitting.
- `INSERT OVERWRITE` is idempotent (overwrites the state dir), so a re-run after a partial failure is safe.

## Testing

- `unload_sql.py`: unit tests for the SELECT expr list (incl. the `NULL AS` Prisma-extra and DDL
  ordering), the per-state `WHERE`, and the exact OPTIONS string. Pure, no I/O.
- `unload.run`: unit test against a fake SDK client (records submitted SQL, returns canned counts) +
  a fake S3 lister, asserting the manifest shape (columns, per_state_row_counts, files) and the
  skip-guard / `--state` / `--skip-submit` behaviors. Matches the fake-client style of `mart_introspect`
  and the moto tests.
- `copy_s3`: update the existing tests' expected import-options string to the new `FORMAT csv` form.
- Live path (real warehouse → real S3) is an env-gated integration test, like `test_integration_pg.py`.

## Non-goals / dependencies

- **DATA-1905** (dedicated S3 bucket + UC external location/storage credential) is the write target.
  The unload *code* uses config-supplied names (`s3_bucket`, the export prefix), so it's buildable and
  unit-testable now; an end-to-end run needs 1905 applied. Tracked as its own spec.
- **DATA-1906** (dbt pre-unload data-quality gate) runs before unload; its own spec. Not invoked by
  the loader CLI (the DAG / runbook sequences it ahead of unload).
- Throughput tuning (file count/size per state) is left to the warehouse + a later pass; the FL-first
  milestone measures it.
