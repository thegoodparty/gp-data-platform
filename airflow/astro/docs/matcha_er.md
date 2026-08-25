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
| `matcha_image_pull_secret` | Image pull secret name from Astronomer support. Unset means the GHCR package is public and the kubelet pulls it anonymously. |

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

## Image pull

The matcha GHCR package is public for now, so the Kubernetes pod pulls it anonymously with no
`image_pull_secrets`. When Astronomer support delivers a per-deployment pull secret, set
`matcha_image_pull_secret` to its name on that deployment — the pod resolves the secret at task
runtime (`pre_execute`, since `image_pull_secrets` cannot be templated), so no redeploy is needed.
Flip the GHCR package back to **private** only once **both** astro-dev and astro-prod have their own
secret set and confirmed working; flipping it earlier breaks whichever deployment doesn't have a
secret yet.

## When a gate fails

A gate failure means the dated table is intact and no live table moved — the run is safe to leave
alone while you investigate. The raised error names the table, the observed value, and the threshold
it missed (row-count ratio, null probe, distinct-id count, id overlap, or missing source). Either
re-run the entity's group after fixing whatever produced bad prematch input, or, if the change is
legitimate (e.g. a source's row volume genuinely shifted), widen the relevant threshold on that
entity's `TableGate` in `matcha_utils.ENTITIES`.

## When a swap crashes midway

The swap sequence pre-drops the leftover `_old` table before renaming the current live table aside, so
a crash mid-swap self-heals on the next run: that run's swap simply drops the stale `_old` again and
proceeds. No manual cleanup is needed.

## Rolling back a bad vintage

The renamed-aside `_old` table holds the previous vintage until `cleanup` runs after
`dbt_build_er_source` succeeds, and dated vintages are otherwise kept for 28 days. To roll back, rename
the vintage you want back into the live name with `ALTER TABLE ... RENAME TO ...` (rename the current
live table aside first if you want to keep it for comparison).

## References

- `airflow/astro/include/custom_functions/matcha_utils.py` — `EntitySpec`, gate logic, and the swap
  SQL builders this DAG calls.
- `matcha/` — the Splink entity-resolution pipeline the container image is built from.
