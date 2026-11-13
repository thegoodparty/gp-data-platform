"""## Load Bifrost gateway usage into the warehouse

Two jobs, and this DAG is the second one. A scheduled job in the `goodparty-workbench` AWS account
reads the Bifrost gateway's own Postgres `logs` table and writes one Parquet file per run to an S3
prefix (`thegoodparty/ops` owns that job; the contract is in
`include/custom_functions/bifrost_export.py`). This DAG lands those files in
`airflow_source.bifrost_request_log`, which dbt then models into `mart_finance`.

### One task, because COPY INTO keeps the bookkeeping

`COPY INTO` is idempotent: Databricks documents that files already loaded are skipped on
subsequent runs, and that this holds even if a file was modified since. So the DAG names the
prefix and lets the warehouse work out what is new. There is no listing task, no per-day mapped
task, no watermark and no anti-join — a re-run loads nothing and a new export's file is the only
thing picked up.

### Airflow holds no AWS credentials

The export never rewrites a file: each run writes one new object whose name carries the run time,
so a file is complete or absent and there is no marker to poll for. Nothing here lists or reads
S3, which means no second grant on the engineer's bucket for Astro — Databricks reads the prefix
through its own Unity Catalog storage credential, and that is the one reader.

### The landing grows by every run's window, and that is accepted

Each run appends a whole 7-day window (90 days monthly), so at 2026-11-04 traffic (~6,791
requests/day) the table grows by roughly 50k rows a run and about 25M rows a year. Staging
dedupes, and the bucket keeps every file, so any day can be reproduced as of any run. The growth
is deliberate: it buys that reproducibility, and it is the price of not rewriting objects.

### Nothing is deleted

The landing table is append-only, like every other job's record here. There is no prune task,
because deleting an observation would make the mart's history for that day diverge the next time
it rebuilds.

### Connections (set in Astro Environment Manager):
- `databricks_conn_id` (Variable) names a Databricks connection (Generic, OAuth M2M).

### Variables:
- `databricks_conn_id` — the Variable naming the Databricks connection.
- `databricks_catalog` — the Unity Catalog catalog.
- `bifrost_landing_schema` — the ingest schema this DAG writes (`airflow_source` in prod,
  `airflow_source_dev` in dev). There is one catalog, so the schema is what keeps a dev run off
  production data.
- `bifrost_export_bucket` / `bifrost_export_prefix` — where the export writes.

### Params:
- `dry_run` — build and log the statement, run nothing.

The DAG deploys `is_paused_upon_creation=True`. Unpausing is the owner-gated activation step, and
it cannot be activated before the export exists.
"""

import logging

from airflow.sdk import Param, Variable, dag, get_current_context, task
from include.custom_functions.bifrost_export import (
    build_copy_sql,
    ensure_landing_table,
    export_prefix,
    land_export,
)
from include.custom_functions.databricks_utils import connect_from_conn_id
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")


@dag(
    dag_id="load_bifrost_usage",
    start_date=datetime(2026, 11, 1, tz="UTC"),
    schedule="30 10 * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-engineering",
        # The export runs about 09:00 UTC and this follows it at 10:30, clear of the 00:02 UTC
        # dbt build (4pm Pacific).
        "retries": 2,
        "retry_delay": duration(minutes=15),
    },
    doc_md=__doc__,
    tags=["bifrost", "finance"],
    params={
        "dry_run": Param(False, type="boolean", description="Log the statement and run nothing."),
    },
)
def load_bifrost_usage():
    @task
    def load() -> int:
        catalog = Variable.get("databricks_catalog").strip()
        schema = Variable.get("bifrost_landing_schema").strip()
        bucket = Variable.get("bifrost_export_bucket").strip()
        prefix = Variable.get("bifrost_export_prefix").strip()

        if get_current_context()["params"].get("dry_run"):
            t_log.info(
                "bifrost dry run: %s",
                build_copy_sql(catalog, schema, export_prefix(bucket, prefix)),
            )
            return 0

        # `connect_from_conn_id` resolves the `databricks_conn_id` Variable itself; passing it a
        # resolved value makes it treat a connection id as another Variable name.
        with connect_from_conn_id() as connection:
            ensure_landing_table(connection, catalog, schema)
            return land_export(connection, catalog, schema, bucket, prefix)

    @task
    def report(loaded: int) -> None:
        t_log.info("bifrost: %d new row(s) landed", loaded)
        if loaded == 0:
            t_log.info(
                "bifrost: no new rows. Expected on a re-run or before the export has written a "
                "new file, since COPY INTO skips files it has already loaded."
            )

    report(load())


load_bifrost_usage()
