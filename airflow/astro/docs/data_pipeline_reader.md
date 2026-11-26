# Data pipeline reader (`data_pipeline_reader`)

Operational reference for the DAG that watches the platform's own pipelines and reports what
they did. It only reads: it triggers nothing, reruns nothing, cancels nothing and pauses
nothing. Diagnosis comes later; so do the Airflow and Airbyte collectors, which are the next two
pull requests on this same DAG.

## What it does

Every five minutes: `load_pipelines` validates the checked-in pipeline list,
`collect_dbt_cloud` asks dbt Cloud for the finished runs of each pipeline the list names,
`send_to_grafana` sends one short record per finished run plus one status update per pipeline to
Grafana over OTLP/HTTP as logs, and `save_state` writes the cursors, the dedupe set and the
per-pipeline streak back to the `data_pipeline_reader_state` Variable.

The pipeline list is `airflow/astro/include/pipelines/pipelines.yaml`. It is data, not code:
adding a pipeline is a YAML edit, and `tests/test_pipelines_registry.py` loads that exact file, so
a malformed or incomplete entry fails the `airflow` workflow instead of reaching Astro as a
reader that quietly watches nothing.

## The one fact that trips people up

A dbt Cloud step artifact must be fetched with `?step=N`. Without the step parameter the API
returns the last step's artifact, and on a multi-step job the last step is often `docs generate`,
whose `run_results.json` looks green while the build is red. The reader takes the step number from
the run's own steps, and `include/reader/dbt_cloud.py` refuses to fetch without one.

## Reading the signal

The status update is a log line with `record_type = status_update` in the body, so a
Grafana-managed recording rule can turn it into a metric. Ticket 5 owns those rules; this DAG only
guarantees the shape.

| What the reader sees | What the status update says |
| --- | --- |
| A run fails | `failed_since_last_success = 1` |
| A retry starts, or a newer run is skipped, cancelled or is a rehearsal | unchanged: a failure stays open |
| A run that counts as a real success finishes | `failed_since_last_success = 0`, `last_real_success_at` = its finish time |
| The reader cannot read the tool | `last_read_ok = 0` and `last_read_at` stays at the last good read, so rule 4 fires; never reported as recovery |

**A manual run does not clear the failure flag.** The nightly's real success is a `scheduled`
run, because a hand-triggered run may be a partial or targeted build. A hand-fixed nightly
therefore stays alerting until the next scheduled run, at most 12 hours later. That is the
deliberate price of never letting a partial rerun read as recovery.

**A cold start rebuilds the streak from the job's own runs**, newest first to the latest real
success. It never assumes healthy: a lost or hand-edited Variable would otherwise turn every open
failure into a recovery that never happened.

## Connections (Astro Environment Manager)

Referred to by id and only used at run time; nothing here inspects them.

- `dbt_cloud_reader`: dbt Cloud read-only service token on the cell host. The Admin API does not
  answer on the generic host, so the client refuses any other.
- `grafana_otlp`: Grafana Cloud OTLP endpoint, instance id as the basic-auth login and the
  logs-write token as the password.
- `astro_api_reader`: the Airflow API. Unused in this PR, created now so the Airflow collector PR
  does not wait on it.

## Variables (Astro Environment Manager)

- `data_pipeline_reader_state`: cursors, the bounded per-tool dedupe set, the per-pipeline streak
  and the last good read time, as JSON. Written at the end of a successful run. Nothing secret
  goes in it. It is safe to delete to force a cold start, which then rebuilds from dbt Cloud's
  own history.

## What a failure means

`send_to_grafana` runs on `all_done` and `save_state` on the default `all_success`, so:

- a collector that fails still produces a status update with `last_read_ok = 0`, and its cursor
  and last read time do not move, so the next run replays the window;
- a failed Grafana send fails the run and leaves state untouched, which is what lets rule 4 tell
  a quiet reader from a quiet platform.

Nothing in this DAG writes to Databricks, reads a row value, or sends a log line. An error line
is redacted before it leaves: emails, phones, long tokens, URL credentials, quoted literals and
long digit runs are stripped.
