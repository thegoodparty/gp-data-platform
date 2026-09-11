# CLAUDE.md

Short, non-obvious context for `anthropic-analytics-loader`. See `README.md` for setup/usage.

## Why standalone, not inside `people-api-loader`

`people-api-loader/CLAUDE.md` says a second consumer should sit alongside `people_api/`, reusing
`loader.core`. That core harness (S3 manifests, Aurora provisioning, RDS cluster lifecycle) is
built for the Databricks -> S3 -> RDS pipeline shape; this loader is API -> Delta only and needs
none of it. Only `databricks_writer.py`'s `run_statement` is intentionally parallel to
`loader/core/databricks.py`'s helper of the same shape -- not shared, to avoid pulling
boto3/psycopg/etc. into a project that never touches AWS.

## Why raw SQL literals, not the databricks-sql-connector

Writes go through the Statement Execution API (`WorkspaceClient().statement_execution`), matching
`people-api-loader`'s pattern -- not `databricks-sql-connector`. That API takes a SQL string, not
Python-level bound parameters, so `databricks_writer.sql_literal()` is the thing standing between
API response data and a SQL injection: any change there needs the tests in
`tests/test_sql_literal.py` to stay green, and any new value type needs a case added there before
it's used as a column value.

## Why `raw_json` on every table

The Enterprise Analytics API's `users` response nests four levels deep (chat/Claude
Code/Cowork/Office/Science metrics). Flattening all of it up front would be a lot of columns for
metrics nobody's asked for yet. Each table keeps the untouched API row in `raw_json` so a dbt model
can pull a not-yet-flattened field out later without a loader change.

## Data floor

The Enterprise Analytics API has no data before 2026-01-01. `cli.py`'s `DATA_FLOOR` clamps
backfills to that regardless of `--start-date`.
