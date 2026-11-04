# dbt project

The dbt project for the GoodParty.org data platform, built on Databricks with the dbt Cloud
CLI. Models follow staging, then intermediate, then marts. Working conventions for this
project are in `CLAUDE.md`.

## Append-only logs, current state in dbt

When a job records what it did (a payload sent, a record merged, a file loaded), it
appends one row per event. It never records state by changing rows in place: no UPDATE,
DELETE or MERGE against its own log.

dbt then rebuilds the current view from that log. Typical shapes:

- The latest event per key, for "what did we last send":

  ```sql
  select *
  from {{ ref("stg_airflow_source__example_log") }}
  qualify row_number() over (partition by tracking_key order by logged_at desc) = 1
  ```

- An anti-join against terminal events, for "what is still outstanding". For example,
  `hubspot_contact_merge_queue` excludes every contact the merge log records as merged.

Why this way:

- **The log is the audit trail.** Every event stays, with its time and run, and Delta
  table history shows nothing was rewritten.
- **A crash loses no history.** A job that dies partway through has appended what it
  finished, and the next run continues from what the log shows.
- **The state rule can change.** "Current" is a query, so changing it is a model change
  that applies to all of history, with no backfill of a state table.
- **dbt can test it.** Log health checks (repeated failures, unexpected statuses) are
  ordinary dbt tests on the source.

## Logs live in ingest schemas

A job's log is ingest. It goes in a schema that exists for data written by something
other than dbt, and dbt reads it as a source.

- **Use a `<writer>_source` schema**, such as `airflow_source` (Airflow-run jobs),
  `er_source` (matcha) or `airbyte_source` (Airbyte). Declare the table in
  `models/staging/__sources.yaml`, and stage it as `stg_<source>__<table>`.
- **Separate dev from prod by schema**, not by table name: `airflow_source_dev` beside
  `airflow_source`, `er_source_dev` beside `er_source`. There is one catalog, so the
  schema is all that keeps a dev run off production data.
- **The writing service principal owns the table.** If a person creates it, for example
  through a job's one-time init command, transfer ownership to the service principal
  afterwards.
- **Never put a log in a schema dbt builds into.** That means `dbt`, `dbt_staging`,
  `dbt_preview`, any `mart_*` schema, personal `dbt_<user>` schemas and
  `dbt_cloud_pr_*` schemas. dbt treats those schemas as its own: it replaces their
  tables on a full refresh, and dbt Cloud creates and drops PR schemas. A log there can
  be lost. Lineage also becomes unclear, because nothing marks the table as a source
  rather than a model.
