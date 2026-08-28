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
| top dbt nodes | `system.query.history` | the specific expensive models/tests, and whether each is scan- or compute-bound |
| high-freq-job estimate | exec-hr in the 4-hourly-only UTC windows ×6/4 | is killing it sufficient? |

$ for the dbt/consumer views is the **SQL-warehouse bill prorated by query
execution time** (serverless bills warehouse uptime, not per query), so those
figures are an allocation, not a metered per-query charge. State that.

## Interpreting results

- **View-test antipattern (usual top driver):** a test on a `view`-materialized
  model re-executes the whole view query. Rank top nodes; if expensive nodes are
  `test` rows whose model is a view, materialize that model as a **table**
  (in `dbt_project.yml` directory config, per `dbt/project/CLAUDE.md`). If the
  mart emits pretty/spaced column names (e.g. a reverse-ETL contract like
  `First Name`), a Delta table rejects them (`DELTA_INVALID_CHARACTERS`) — add
  `+tblproperties: {delta.columnMapping.mode: "name"}` to keep the names.
- **Compute-bound nodes (nested-loop joins):** read the `hr/TB` and `profile`
  columns before proposing a fix. A node with a *high* `hr/TB` (say >200) is
  burning CPU without reading much — the plan is wrong, and pruning, partitioning,
  clustering and incremental will all miss it. The usual cause is a join Spark
  could not hash: most often a correlated `not exists` / `not in` whose predicate
  **ORs together several different keys**, which cannot become a hash anti-join and
  plans as `BroadcastNestedLoopJoin LeftAnti` (left rows × right rows comparisons,
  with any `regexp_replace`/`lower`/`trim` in the predicate re-evaluated per pair).
  Confirm with `EXPLAIN <compiled sql> | grep -i nestedloop`, then rewrite as one
  hash anti-join per key against pre-normalized `distinct` key sets, unioned or
  left-joined and filtered on `is null`. Normalizing each side once in a CTE is
  half the win; making the join hashable is the other half.
- **Frequency is a multiplier:** the dbt Cloud models-built meter = models/run ×
  runs. A job of N models run 6×/day bills 6N. Incremental materialization cuts
  Databricks compute but **not** the models-built count.
- **Reach for incremental only when the cost is in the write.** If a node writes
  little but reads or computes a lot, incremental adds state and reload complexity
  without touching the bill. It is also the wrong tool for a *rolling-window
  exclusion feed* (rows must leave as they age out or get claimed downstream) —
  an append would retain stale rows, and a merge-with-deletes rescans anyway.
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
- **Use `execution_duration_ms`, not `total_duration_ms`.** The total also carries
  `waiting_at_capacity_duration_ms` (queued behind other statements) and
  `waiting_for_compute_duration_ms` (warehouse starting). On a saturated warehouse
  that inflates cheap statements that merely queued behind an expensive one, and
  you end up "fixing" the wrong node. Pull the components alongside the total when
  a duration looks implausible for the work involved.
- **Statement count is not build count.** dbt stamps its `SHOW`/`OPTIMIZE`/metadata
  statements with the same `node_id` as the build itself, so a node showing ~800
  statements may be ~150 real builds plus ~650 trivial ones. Use the `>60s` column,
  or filter `statement_type` (`REPLACE`/`MERGE`/`CREATE`), before concluding
  anything about frequency.
- **The same `node_id` spans environments.** `target_name` in the dbt comment
  separates `prod` from `ci` from a developer's own runs. A node that looks
  expensive can be mostly PR-CI rebuilds in `dbt_cloud_pr_*` schemas, which is a
  different problem (and a different fix) than an expensive production job.
