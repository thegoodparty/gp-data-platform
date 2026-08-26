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
| `matcha_image_tag` | matcha image tag to run. Defaults to `latest` if unset; set to a sha to pin a deployment without a code change or a deploy. |

**Connections:** `databricks` / `databricks_dev` (Generic, OAuth M2M) and `dbt_cloud`, both shared with
the other DAGs.

**Pool:** create `matcha_er` with **1 slot** on each deployment (Admin -> Pools) before unpausing the
DAG — a task assigned to a pool that doesn't exist fails at scheduling, not gracefully. Each pod requests
8Gi memory / 4 CPU, and three running in parallel would ask for 24Gi against a 20Gi deployment quota, so
the pool holds them to one at a time. This is a quota accommodation, not a modeling decision — within
this DAG the three entities have no dependency on each other and would otherwise run concurrently.
Raising the quota and widening the pool needs no DAG change.

**Concurrency:** the DAG also sets `max_active_runs=1`. `gate`/`swap`/`cleanup` are not pooled — only the
match pods are — so without this, an overlapping manual trigger would give two runs with different
`ds_nodash` whose swaps could interleave DROP/RENAME statements against the same live table.

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
swap is gone a week later. There is no dated-vintage fallback for this: a table that swapped successfully
no longer has a dated vintage (the rename consumed it), only whatever `_old`/Delta-history recovery
paths "Rolling back a bad vintage" describes.

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
transaction): `DROP _old` -> `RENAME live -> _old` -> `RENAME dated -> live`. The `swap` task calls this
once for the cluster table, then once for pairwise. Two different things can crash midway, and
`swap_table` guards on the DATED table's existence specifically so retrying either is safe:

- **Crash within one table's own three-statement sequence**, before its final rename commits. If it
  crashes before the live table is renamed away, the live table is untouched and the next attempt just
  re-drops the (already-gone) `_old` and proceeds normally. If it crashes **after** the live table has
  been renamed to `_old` but before the dated table takes its place, there is briefly **no live table at
  all** for that table — every downstream dbt model and civics mart reading it fails until the swap
  completes. Either way, the dated table for that specific table is still there, so `swap_table` on retry
  sees the live table missing, runs only the drop-`_old` + rename-dated-into-place pair, and the live
  table comes back. The `swap` task's own retries (2, at the DAG's default 10-minute delay) normally
  clear this automatically within about 20 minutes with no manual step.
- **One table finishes and the other then raises** (e.g. cluster promotes fully, pairwise hits a
  transient error) — the whole `swap` task fails and Airflow retries it from the top, calling
  `swap_table` again for BOTH tables. For the table that already finished, its dated table is gone
  (a completed swap consumes it): `swap_table` checks the dated table first, sees it is gone and the
  live table is present, logs that it is already promoted, and returns without touching anything. Only
  the table that actually failed gets swapped for real. This guard is what makes retrying safe — without
  it, re-running drop+rename against an already-promoted table would destroy the `_old` backup for
  nothing and then fail trying to rename a dated table that no longer exists.

Check whether a table's live version is currently missing:

```sql
SELECT 1 FROM <catalog>.information_schema.tables
WHERE table_schema = 'er_source' AND table_name = '<table>';
-- no row back means the live table is missing right now
```

If every retry is exhausted, or you need it back sooner:

- **Clear the `swap` task** for that entity in the Airflow UI to let it try again. This is safe in both
  crash shapes above because of the dated-table guard described there — a table already promoted is
  skipped rather than re-swapped, and only tables that genuinely still need swapping are touched.
- **Rename `_old` back into place by hand** if you want the pre-crash vintage restored immediately
  rather than waiting on the retry to install the new one. **Do this before the automatic retry fires**
  (10 minutes after the failure): the retry's own `swap_table` call drops `_old` as its first statement,
  so once that retry runs there is nothing left to rename back. Mark the task instance failed (not just
  cleared) or otherwise stop the automatic retry first if you need this window preserved:

```sql
ALTER TABLE <catalog>.er_source.<table>_old RENAME TO <catalog>.er_source.<table>;
```

## Rolling back a bad vintage

**A dated vintage that swaps successfully stops existing as a separate table** — the rename consumes it,
so it IS the live table afterward. The 28-day retention in `cleanup` only ever protects vintages that
were never promoted: a gate failure, a still-failing entity, or a rehearsal run where `matcha_swap_enabled`
withheld the rename. **In live steady state, once `cleanup` has run for a given week, there is no dated
vintage and no `_old` left for that table — nothing for the rollback SQL below to find.** Know which
recovery path applies before reaching for one at 3am:

1. **`_old` still exists** (the window between a live swap and the next `cleanup` run, or after a
   crashed-and-not-yet-cleaned-up swap): the fastest path, and the one to check first.
   ```sql
   SELECT 1 FROM <catalog>.information_schema.tables
   WHERE table_schema = 'er_source' AND table_name = '<table>_old';
   ```
   If it exists:
   ```sql
   -- 1. Move the current, bad live table out of the way
   ALTER TABLE <catalog>.er_source.<table> RENAME TO <catalog>.er_source.<table>_bad_<yyyymmdd>;
   -- 2. Rename the prior vintage back into the live name
   ALTER TABLE <catalog>.er_source.<table>_old RENAME TO <catalog>.er_source.<table>;
   -- Repeat for the matching pairwise_/clustered_ counterpart.
   ```

2. **`_old` is already gone: Unity Catalog `UNDROP TABLE`.** `cleanup`'s `DROP TABLE IF EXISTS` on `_old`
   is recoverable for **7 days** after the drop:
   ```sql
   UNDROP TABLE <catalog>.er_source.<table>_old;
   ```
   then follow the two-statement rename sequence in path 1 once it's back.

**Past the 7-day undrop window, there is no rollback table left.** Delta time travel does NOT help here:
every vintage is a table matcha built fresh that run (`CREATE OR REPLACE TABLE` + `COPY INTO`), and a
swap RENAMEs it into place rather than writing new versions onto the live name's existing history — so
whatever table currently holds the live name has its OWN history starting from this run's create, not a
record of prior weeks. Restoring the live table to an earlier version would only wind it back to its own
pre-`COPY INTO` empty state, not to a previous vintage. Once `_old` is gone and 7 days have passed, the
only recovery is a full re-match of that entity.

Whichever path recovers the table, **re-run `dbt_build_er_source`** (or trigger the underlying dbt Cloud
job's `dbt build --select path:models/staging/er_source+` step directly) before calling the rollback
done. Restoring the ER table is not enough on its own — every downstream mart still has the bad
vintage's output baked in until that build runs again, which is the state most likely to be misread as
"the rollback didn't work."

## References

- `airflow/astro/include/custom_functions/matcha_utils.py` — `EntitySpec`, gate logic, and the swap
  SQL builders this DAG calls.
- `matcha/` — the Splink entity-resolution pipeline the container image is built from.
