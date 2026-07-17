---
name: dbt-databricks-cost-audit
description: Use when investigating a Databricks or dbt Cloud cost/usage increase, attributing spend across warehouses, clusters, jobs, models, and tests, or answering "what is driving our compute cost". Produces an interactive HTML cost dashboard from system tables.
---

# dbt / Databricks cost audit

## Overview

Attribute Databricks (and dbt Cloud) spend to its real drivers and produce an
interactive HTML dashboard that slices cost monthly and weekly by product,
by consumer, and by dbt node type (model vs **test** vs snapshot). Built to
answer questions like "how much of our cost is testing?" and "is it enough to
just slow down the high-frequency dbt job?".

Core idea: **dbt is usually the dominant Databricks consumer, and within dbt,
tests on view-materialized models plus high build frequency dominate.** The
Databricks bill meters *compute time*; the dbt Cloud bill meters *successful
models built* (count). They move for different reasons — separate them.

## Prerequisites

- Databricks CLI configured with a workspace profile that can read
  `system.billing.*` and `system.query.history` (verify: `databricks current-user me -p <profile>`).
- A SQL warehouse id to run queries against (`databricks warehouses list -p <profile>`).
- Optional (dbt Cloud side): a service token + account id for the Admin API
  (`https://<host>/api/v2/accounts/<id>/jobs/`), to read job selectors/schedules.

## Quick start

```bash
python scripts/cost_audit.py --profile <profile> --warehouse <warehouse_id> \
    --since 2026-01-01 --out cost_dashboard.html
# iterate on the visualization without re-billing queries:
python scripts/cost_audit.py --warehouse <id> --from-json cost_dashboard.json --out cost_dashboard.html
```

Outputs `cost_dashboard.html` (self-contained, opens in any browser; theme-aware)
and `cost_dashboard.json` (raw pulled data). **The output contains internal cost
figures — treat as internal, do not publish externally without sign-off.** Do not
commit generated `cost_dashboard.*`; commit only the tool.

## What it pulls

| Query | Source | Answers |
|---|---|---|
| cost by product / month + week | `system.billing.usage` × `list_prices` | overall trend, SQL vs ALL_PURPOSE vs APPS… |
| dbt exec-hr by node type | `system.query.history` (`statement_text` node_id) | **testing vs model vs snapshot cost** |
| warehouse exec-hr by consumer | `system.query.history` `client_application` | dbt vs airbyte vs BI vs dev |
| top dbt nodes | `system.query.history` | the specific expensive models/tests |
| high-freq-job estimate | exec-hr in the 4-hourly-only UTC windows ×6/4 | is killing it sufficient? |

$ for the dbt/consumer views is the **SQL-warehouse bill prorated by query
execution time** (serverless bills warehouse uptime, not per query), so those
figures are an allocation, not a metered per-query charge. State that.

## Interpreting results

- **View-test antipattern (usual top driver):** a test on a `view`-materialized
  model re-executes the whole view query. Rank top nodes; if expensive nodes are
  `test` rows whose model is a view, materialize that model as a **table**
  (in `dbt_project.yml` directory config, per `dbt/project/CLAUDE.md`).
- **Frequency is a multiplier:** the dbt Cloud models-built meter = models/run ×
  runs. A job of N models run 6×/day bills 6N. Incremental materialization cuts
  Databricks compute but **not** the models-built count.
- **Is slowing the high-freq job enough?** Compare its estimate against total
  testing cost. If tests (which also run in the twice-daily main job) exceed the
  high-freq job, no — you must also decouple the full test suite from every build.

## Gotchas (learned the hard way)

- A dbt Cloud run writes **one** `run_results.json`, overwritten by its last step.
  Jobs ending in `dbt docs generate` leave it reflecting the whole-project
  *compile* (ignores `--exclude`), not the build. To measure what a job actually
  built, parse the `dbt build` step's own debug log (`OK created … model`), not
  `run_results.json`.
- dbt Cloud job selectors/schedules live only in dbt Cloud — pull them via the
  Admin API `jobs/` endpoint (`execute_steps`, `schedule.cron`, `triggers`).
- `system.query.history` column is `statement_text` (not `query_text`); dbt stamps
  each query with `"node_id": "..."` in a leading comment — regexp-extract it to
  attribute cost to a model/test.
