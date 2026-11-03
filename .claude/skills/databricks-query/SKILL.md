---
name: databricks-query
description: Use when querying Databricks, running SQL, exploring catalogs/schemas/tables, or doing EDA against GoodParty data in goodparty_data_catalog. Databricks-specific keywords - SQL warehouse, databricks CLI, Unity Catalog, statement execution.
---

# Databricks Query

Query Databricks through the official `databricks` CLI over U2M OAuth. No repo checkout, no Python `DatabricksClient`, no personal access token.

## Auth (one-time, then occasional refresh)

OAuth profile `dbc-3d8ca484-79f3` (host `dbc-3d8ca484-79f3.cloud.databricks.com`). Minting or refreshing the token is an interactive browser flow, so the **human must run it**:

```bash
databricks auth login --profile dbc-3d8ca484-79f3
```

In a gp-pi container, the engineer runs `dbx-login` in a `gp-pi shell` instead, and the agent never starts a browser login itself.

Re-run that whenever a query fails with `token expired` or `stored credentials from older CLI versions`. Confirm with `databricks auth profiles` (want `Valid: YES`).

## Run SQL

The CLI has **no** "run a query" command. SQL executes through the Statement Execution REST API. Use the helper, which json-encodes the SQL, pins the warehouse, drops any stale `DATABRICKS_TOKEN`, polls, and pages result chunks:

```bash
# from the gp-data-platform repo root (or prefix the path with
# "$(git rev-parse --show-toplevel)/" to run from anywhere in the repo)
python .claude/skills/databricks-query/scripts/dbsql.py \
  "SELECT count(*) FROM goodparty_data_catalog.dbt.stg_airbyte_source__gp_api_db_campaign"

# multi-line SQL: pass a file or pipe stdin
python .claude/skills/databricks-query/scripts/dbsql.py -f /tmp/query.sql
```

The helper is stdlib-only and shells out to the `databricks` CLI (no venv or extra packages). Flags: `--csv`, `--json` (raw rows), `--catalog`/`--schema` (defaults for unqualified names), `--limit N`. Run with `--help` for all. Default output is an aligned table.

### Raw API (fallback, and the pattern to port to Codex/other agents)

```bash
env -u DATABRICKS_TOKEN databricks api post /api/2.0/sql/statements -p dbc-3d8ca484-79f3 --json '{
  "warehouse_id": "18583d8b081c6486",
  "statement": "SELECT 1",
  "wait_timeout": "50s"
}'
```

Rows land in `result.data_array`; column names in `manifest.schema.columns`. If `status.state` is `PENDING`/`RUNNING`, poll `databricks api get /api/2.0/sql/statements/<statement_id>`.

## Workspace reference

| Thing | Value |
|-------|-------|
| OAuth profile | `dbc-3d8ca484-79f3` |
| SQL warehouse id | `18583d8b081c6486` (serverless, auto-starts) |
| Main catalog | `goodparty_data_catalog` |

| Catalog.schema | Contents |
|----------------|----------|
| `goodparty_data_catalog.dbt` | dbt models (staging, intermediate, marts) |
| `goodparty_data_catalog.model_predictions` | ML model outputs |
| `goodparty_data_catalog.private_nigel` | ad-hoc analysis tables |

Discover structure by querying `goodparty_data_catalog.information_schema.tables` / `.columns` (use `ORDER BY ordinal_position` for column order), or via `databricks schemas list` / `databricks tables list` (each takes `--help` for args). JSON columns use Databricks colon syntax: `details:office::string`.

## Beyond SQL (non-SQL CLI commands)

All read-only and gated by your OAuth identity. Prefix with `env -u DATABRICKS_TOKEN` and pass `-p dbc-3d8ca484-79f3` (or `export DATABRICKS_CONFIG_PROFILE=dbc-3d8ca484-79f3` once per shell).

| Need | Command |
|------|---------|
| Describe a table (columns, types, comments) without SQL | `databricks tables get goodparty_data_catalog.dbt.<table>` |
| Browse schemas / tables | `databricks schemas list goodparty_data_catalog` · `databricks tables list goodparty_data_catalog dbt` |
| Warehouse state and size | `databricks warehouses list` |
| Recent SQL (debug, perf) | `databricks query-history list` |
| Orchestration jobs | `databricks jobs list` · `databricks jobs get <id>` |
| Lakeview dashboards | `databricks lakeview list` · `databricks lakeview get <id>` |
| Anything else in the REST API | `databricks api get/post <path>` |

Other groups exist (`genie`, `pipelines`, `clusters`, `experiments`, `model-registry`, ...); run `databricks <group> --help`.

## Common mistakes

| Symptom | Fix |
|---------|-----|
| `unknown command "query"` / `"sql"` | There is no SQL subcommand. Execute via `api post /api/2.0/sql/statements` (or the helper). |
| `cannot configure default credentials` | Pass the profile (`-p dbc-3d8ca484-79f3`), don't rely on the empty DEFAULT. |
| `token expired` / `older CLI versions` | Re-run `databricks auth login --profile dbc-3d8ca484-79f3`. |
| auth error citing a `dapi…` token | A stale `DATABRICKS_TOKEN` is in the env; prefix `env -u DATABRICKS_TOKEN` (the helper already does). |
| `--profile` swallows the next word (zsh) | zsh does not word-split `$var`; write the flag literally or set `DATABRICKS_CONFIG_PROFILE=`. |
| Query seems to hang | Large/slow result; the helper polls. For huge scans add a `LIMIT` or `--csv > file`. |

For very long queries, run the helper in the background with its output redirected to a file, and read the file when it finishes.
