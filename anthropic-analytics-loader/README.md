# anthropic-analytics-loader

Pulls Claude Enterprise Analytics data (org usage, org cost, per-user cost, daily user
activity/adoption, org-wide summaries) into Delta tables, for the Sigma "Claude spend" template
([help.sigmacomputing.com/docs/claude-spend-template](https://help.sigmacomputing.com/docs/claude-spend-template)).

This is a different API from the Console/Platform Usage & Cost Admin API: it requires an
**Analytics API key** (`read:analytics` scope), created by the org's primary owner at
[claude.ai/admin-settings/api-access](https://claude.ai/admin-settings/api-access) -- an Admin API
key cannot call these endpoints.

## Setup

```bash
uv sync
cp .env.example .env   # fill in ANTHROPIC_ANALYTICS_API_KEY and LOADER_DATABRICKS_WAREHOUSE_ID
```

Databricks auth is ambient (not read from `.env`): locally it comes from `~/.databrickscfg`
(`databricks auth login`), matching `people-api-loader`'s convention; in Airflow it would come
from `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` templated from the
shared Databricks connection, the same way `people-api-loader`'s `unload` step gets them.

## Usage

```bash
# Backfill everything available (data exists from 2026-01-01 onward)
uv run anthropic-analytics-loader sync --start-date 2026-01-01 --end-date 2026-09-11

# Daily/incremental run (defaults to the last 3 days, re-syncing to pick up late corrections)
uv run anthropic-analytics-loader sync

# Sync a subset of tables
uv run anthropic-analytics-loader sync --table org_cost_report --table summaries
```

Each run is idempotent: it deletes the date range being synced from each target table, then
re-inserts, so re-running (or overlapping backfill/daily windows) never duplicates rows.

## Tables written

All land in `LOADER_CATALOG.LOADER_SCHEMA` (default `goodparty_data_catalog.dbt_audrey`, Audrey's
dbt dev schema -- promote to a real dbt source/schema once this is validated):

| Table | Source endpoint | Sigma template maps to |
|---|---|---|
| `anthropic_analytics_org_usage_report` | `analytics/usage_report`, by product+model | `ANTHROPIC_ANALYTICS_ORG_USAGE_REPORT` |
| `anthropic_analytics_org_cost_report` | `analytics/cost_report`, by product+model+cost_type | `ANTHROPIC_ANALYTICS_ORG_COST_REPORT` |
| `anthropic_analytics_user_cost_report` | `analytics/user_cost_report` | `ANTHROPIC_ANALYTICS_USER_COST_REPORT` |
| `anthropic_analytics_users_daily` | `analytics/users`, one row per user per day | `ANTHROPIC_ANALYTICS_USERS_DAILY` |
| `anthropic_analytics_summaries` | `analytics/summaries` | `ANTHROPIC_ANALYTICS_SUMMARIES` |

Every table also has a `raw_json` column with the full API row, so a metric we haven't flattened
into its own column yet is still queryable (`get_json_object(raw_json, '$.chat_metrics...')`) and
nothing is lost if Anthropic adds fields later. `users_daily` in particular only flattens the
top-level chat/Claude-Code/Cowork counts -- `office_metrics` (per Office app) and `science_metrics`
are in `raw_json` only for now.

Not yet built: `ANTHROPIC_ANALYTICS_SKILLS_DAILY` and `ANTHROPIC_ANALYTICS_CONNECTORS_DAILY` don't
have their own endpoints -- skill/connector counts live nested inside `analytics/users` rows
(`raw_json` on `users_daily`). A dbt model can unnest those from `raw_json` into the two extra
tables Sigma wants, once this loader's core tables are validated end-to-end.

## Not done here

Following `people-api-loader`'s split between the loader CLI and its Airflow wiring: this repo
only has the CLI. Scheduling it (an Airflow DAG on Astro, with the `databricks_conn_id` /
`ENVIRONMENT` plumbing `people-api-loader/CLAUDE.md` describes) is a separate step that needs real
infra decisions (which SQL warehouse, which service principal) this loader doesn't make on its own.
