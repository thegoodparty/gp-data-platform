# Matcha entity-resolution DAG (`matcha_er`)

Design and operational reference for the DAG that runs matcha's Splink entity resolution over the
three matched entity types (candidacy stages, elected officials, election stages) and swaps the
results into the `er_source` tables dbt reads.

## What it does

Weekly, the DAG refreshes the three dbt "prematch" models via dbt Cloud, then for each entity type
runs the containerized Splink matcher as a Kubernetes pod, quality-gates what it produced, and swaps
the gated output into the live table dbt reads. Once all three entities have swapped, it rebuilds the
downstream `er_source`-dependent dbt models, then runs a cleanup step that drops the prior run's
renamed-aside tables and any dated vintages past the retention window. The five stages are:
`dbt_refresh_prematch` -> three parallel `match -> gate -> swap` entity groups -> `dbt_build_er_source`
-> `cleanup`.

## Why dated tables

The matcha container never writes directly to a live table. Its upload is `CREATE OR REPLACE TABLE`
followed by `COPY INTO`, so if it wrote straight to the name dbt reads, a mid-upload crash would leave
that table empty or half-loaded for every downstream model. Instead each run writes a table dated with
the run's `ds_nodash` (e.g. `clustered_candidacy_stages_20260825`), and the DAG only renames a dated
table into the live name after it passes its gate — a crash during the container run leaves the live
table exactly as it was.

## Variables and pool

Set on the Astro deployment as **Airflow Variables**:

| Variable | Purpose |
|---|---|
| `databricks_conn_id` | Selects the Databricks connection (`databricks_dev` / `databricks`). |
| `databricks_catalog` | Databricks catalog name the ER tables live in. |
| `dbt_cloud_job_id` | dbt Cloud job the bookend `DbtCloudRunJobOperator` tasks run steps against. |
| `matcha_swap_enabled` | Cutover switch. Anything but `"true"` withholds the swap. |

**Connections:** `databricks` / `databricks_dev` (Generic, OAuth M2M) and `dbt_cloud`, both shared with
the other DAGs.

**Pool:** `matcha_er`, one slot. Each pod requests 8Gi memory / 4 CPU, and three running in parallel
would ask for 24Gi against a 20Gi deployment quota, so the pool holds them to one at a time. This is a
quota accommodation, not a modeling decision — the three entities have no dependency on each other and
would otherwise run concurrently. Raising the quota and widening the pool needs no DAG change.

## Rehearsal vs. live

Every run matches, gates, and writes the dated vintage regardless of `matcha_swap_enabled`. The swap
task checks the Variable and, unless it reads exactly `"true"`, logs that it is skipping and leaves the
live tables untouched. That makes every run a full dress rehearsal — the same container, the same
gates, the same dbt rebuild input available for inspection — until an operator deliberately flips the
switch. Leave it unset for verification runs; set it to `"true"` only once a run's dated tables have
been reviewed and are trusted to go live.

**Rehearsal does not pause `cleanup`.** `cleanup` drops each table's renamed-aside `_old` unconditionally,
whether or not that week's swap actually ran. If a deployment does one live swap and then goes back to
rehearsal mode (e.g. to test a matcha change before the next real cutover), the next rehearsal run's
`cleanup` still drops `_old` even though no swap replaced it — the rollback position from the last live
swap is gone a week later with nothing to show for it. The fallback in that case is a dated vintage:
they are kept for 28 days regardless of rehearsal/live status, so the pre-swap state can still be
recovered from the dated table matching the last live swap's run date (see "Rolling back a bad vintage").

## Image pull

The matcha GHCR package is permanently public. The Kubernetes pod carries no `image_pull_secrets`, and
the kubelet pulls the image anonymously on every deployment — there is no per-deployment secret to
provision and no Astronomer support ticket involved.

## When a gate fails

A gate failure means the dated table is intact and no live table moved — the run is safe to leave
alone while you investigate. The raised error names the table, the observed value, and the threshold
it missed (row-count ratio, null probe, distinct-id count, id overlap, or missing source). Either
re-run the entity's group after fixing whatever produced bad prematch input, or, if the change is
legitimate (e.g. a source's row volume genuinely shifted), widen the relevant threshold on that
entity's `TableGate` in `matcha_utils.ENTITIES`.

If the fix is upstream of the three prematch models themselves (a staging model or an earlier layer),
re-running this DAG alone will not rebuild it: `dbt_refresh_prematch` runs
`dbt build --select int__er_prematch_candidacy_stages int__er_prematch_elected_officials int__er_prematch_election_stages`
with no `+` prefix, so it deliberately rebuilds only those three models, not their upstreams. Fix and
build the upstream layer separately (a manual `dbt build --select <upstream>+` or its own job) before
re-triggering `matcha_er`.

## When a swap crashes midway

`swap_statements` runs three statements in sequence per table (Unity Catalog has no multi-statement
transaction): `DROP _old` -> `RENAME live -> _old` -> `RENAME dated -> live`. Where the crash lands
matters:

- **Before or during the drop, or after the drop but before the rename-away** (statement 1, or between
  1 and 2): the live table is never touched. The next attempt just re-drops the (already-gone) `_old`
  and proceeds normally. No manual action needed.
- **After the live table has been renamed to `_old` but before the dated table takes its place**
  (between statement 2 and 3): there is briefly **no live table at all** for that entity's cluster or
  pairwise table — every downstream dbt model and civics mart reading `er_source.clustered_*` /
  `pairwise_*` for it fails until the swap completes. This is the state worth actually checking for.

Check whether the live table is currently missing:

```sql
SELECT 1 FROM <catalog>.information_schema.tables
WHERE table_schema = 'er_source' AND table_name = '<table>';
-- no row back means the live table is missing right now
```

The `swap` task's own retries (2, at the DAG's default 10-minute delay) normally clear this
automatically within about 20 minutes with no manual step: on retry, `swap_table` sees the live table
does not exist, so it runs only the drop-`_old` + rename-dated-into-place pair and the live table comes
back. If every retry is exhausted, or you need it back sooner:

- **Clear the `swap` task** for that entity in the Airflow UI to let it try again — safe, since the
  dated table matcha wrote is untouched by the crash, or
- **Rename `_old` back into place by hand** if you want the pre-crash vintage restored immediately
  rather than waiting on the retry to install the new one:

```sql
ALTER TABLE <catalog>.er_source.<table>_old RENAME TO <catalog>.er_source.<table>;
```

## Rolling back a bad vintage

Dated vintages are kept for 28 days; the immediate prior vintage is also available as `<table>_old`
until `cleanup` runs after `dbt_build_er_source` succeeds. To roll back, rename the vintage you want
back into the live name, having first renamed the current (bad) live table aside so it is not lost:

```sql
-- 1. Move the current, bad live table out of the way (per table: cluster and pairwise)
ALTER TABLE <catalog>.er_source.clustered_candidacy_stages
  RENAME TO <catalog>.er_source.clustered_candidacy_stages_bad_<yyyymmdd>;

-- 2. Rename the vintage you want back (a dated table, or `_old` if it's the immediate prior one)
--    into the live name
ALTER TABLE <catalog>.er_source.clustered_candidacy_stages_<yyyymmdd>
  RENAME TO <catalog>.er_source.clustered_candidacy_stages;

-- Repeat both for the matching pairwise_ table.
```

**Then re-run `dbt_build_er_source`** (or trigger the underlying dbt Cloud job's
`dbt build --select path:models/staging/er_source+` step directly) before calling the rollback done.
Renaming the ER table back is not enough on its own — every downstream mart still has the bad
vintage's output baked in until that build runs again, which is the state most likely to be misread as
"the rollback didn't work."

## References

- `airflow/astro/include/custom_functions/matcha_utils.py` — `EntitySpec`, gate logic, and the swap
  SQL builders this DAG calls.
- `matcha/` — the Splink entity-resolution pipeline the container image is built from.
