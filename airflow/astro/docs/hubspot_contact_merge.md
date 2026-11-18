# HubSpot contact merge DAG (`hubspot_contact_merge`)

The DAG runs the `hsm` container from `hubspot-merge/`. It merges duplicate HubSpot
contacts from a pair relation, and appends one row per pair to the merge log. The design
is in `docs/hubspot-contact-merge-job-design.md`.

A merge cannot be undone. The DAG deploys paused, and a scheduled run reads the queue only
once it is unpaused.

## Trigger params

| param | default | use |
|---|---|---|
| `command` | `dry_run` | `dry_run`, `run`, `verify` or `init_log`. A scheduled run always runs `run`. |
| `pair_relation` | empty | read pairs from this relation instead of the Variable's |
| `max_pairs` | empty | cap this run instead of the Variable's |
| `run_id` | empty | the run to check, for `verify` |

## Variables

| Variable | value |
|---|---|
| `hubspot_merge_image_tag` | the commit sha of the image to run. Required. |
| `hubspot_merge_image_pull_secret` | `ghcr-pull` |
| `hubspot_merge_hubspot_token` | the portal's merge service key |
| `hubspot_merge_expected_portal_id` | `51780263` on astro-dev, `21589597` on astro-prod |
| `hubspot_merge_log_table` | `goodparty_data_catalog.airflow_source_dev.hubspot_contact_merge_log` on astro-dev, `goodparty_data_catalog.airflow_source.hubspot_contact_merge_log` on astro-prod. No default. |
| `hubspot_merge_pair_relation` | the queue view, once it exists |
| `hubspot_merge_max_pairs` | the scheduled cap |
| `hubspot_merge_max_rps` | merge calls per second. Defaults to 5. |

Databricks credentials come from the connection the `databricks_conn_id` Variable names.

## First run on a deployment

Trigger with `command` = `init_log`. The deployment's service principal creates the log
table, so it owns it. A rerun changes nothing. `init_log` and `verify` do not need the
pair relation or the cap, so `hubspot_merge_pair_relation` and `hubspot_merge_max_pairs`
can stay unset until the queue exists.

## A hand-checked batch

1. Build the pairs as a table. The Airflow service principals can read any schema in the
   catalog, so your dev schema works.

   ```sql
   create or replace table goodparty_data_catalog.dbt_<you>.merge_batch_01 as
   select primary_hs_contact_id, hs_contact_id, gp_person_id, match_evidence,
          cast(null as date) as latest_election_date
   from goodparty_data_catalog.mart_sales_reverse_etl.hubspot_contact_merge_candidates
   where hs_contact_id in ('...')
   ```

   The job needs those five columns. It merges every row, up to the cap, oldest
   `latest_election_date` first.

2. Look at each pair in HubSpot.
3. Trigger `dry_run` with `pair_relation` set to the table. The task log has one line per
   pair: `would_merge`, or the status the read settles.
4. Trigger `run` with the same `pair_relation`. The last line of the task log is
   `run <run_id>: <status counts>`.
5. Trigger `verify` with that `run_id`. It fails if a `merged` pair does not read as
   merged in HubSpot.
6. Look at the survivors in HubSpot, and at the log:

   ```sql
   select secondary_id, status, survivor_id, error
   from goodparty_data_catalog.airflow_source.hubspot_contact_merge_log
   where run_id = '<run_id>'
   ```

## Exit codes

`run` fails in these cases:

- A merge returned an id other than the primary's (`id_changed`). The ID preservation beta
  is off. The run stops after that chunk. Pause the DAG and look before the next run.
- Merges were tried and none succeeded. The key may lack the write scope.
- The pair table lists a secondary twice, or an id as both primary and secondary.
- The log table is missing or cannot be written. This is checked before the first merge.

`verify` fails when a merged pair does not read as merged. A `failed` row alone does not
fail the run. The pair stays in the queue, and the next run retries it.
