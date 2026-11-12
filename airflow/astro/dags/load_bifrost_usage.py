"""## Load Bifrost gateway usage into the warehouse

Two jobs, and this DAG is the second one. An in-VPC export in the `goodparty-workbench` AWS
account reads the Bifrost gateway's own Postgres `logs` table and writes partitioned Parquet to
an S3 prefix that account owns (`thegoodparty/ops` owns that job; the contract is in
`include/custom_functions/bifrost_export.py`). This DAG reads those partitions and lands them in
`airflow_source.bifrost_request_log`, which dbt then models into `mart_finance`.

    list_days -> load_day (one mapped task per day) -> summarize

### Why this DAG exists at all, rather than a dbt source over the bucket

A dbt source reading S3 directly would need a Unity Catalog external location, which is
Terraform in a different repository, and no model in this repo uses `read_files`. Landing the
rows first keeps the existing boundary: the table lives in a schema the airflow service
principal owns, and dbt reads it as an ordinary source.

### Every marked day in the window, every run

There is deliberately no "newest landed" watermark. The export re-emits a trailing window of
whole days, so a watermark would skip the re-emitted copy of a day — and a request first seen
`processing` would never be re-read as its terminal self, which is the state that carries its
cost. Re-loading is cheap because the INSERT is guarded by an anti-join on `(id, status, cost)`:
unchanged rows are not re-inserted, changed ones are. See `bifrost_export.build_insert_sql`.

### Nothing is deleted

The landing table is append-only, like every other job's record here. There is no prune task,
because deleting an old observation would make the mart's history for that day diverge the next
time it rebuilds. The anti-join is what keeps growth proportional to *changes* rather than to
windows.

### A day is only read once it is complete

The export writes `dt=<day>/_SUCCESS` when a day is finished, and this DAG skips any day without
it. An export re-emits whole days in place, so without that marker a load overlapping an export
would read a half-written partition and nothing downstream would notice.

### The gateway's database is small, and shared

Its RDS instance is a `t4g.micro` behind the one gateway every engineer codes through. The
export's constraints (index-friendly predicate on `timestamp`, only the projected columns,
off-peak hour) live on the ops side; this DAG's job is to not add load by re-reading every day
it can. `full_reload` exists for the monthly re-observation of repriced rows and for a backfill,
not for routine runs.

### Connections (set in Astro Environment Manager):
- The Databricks connection `databricks_conn_id` names (Generic, OAuth M2M).

### Variables:
- `databricks_conn_id` — selects the Databricks connection.
- `databricks_catalog` — the Unity Catalog catalog.
- `bifrost_landing_schema` — the ingest schema this DAG writes (`airflow_source` in prod,
  `airflow_source_dev` in dev). There is one catalog, so the schema is what keeps a dev run off
  production data.
- `bifrost_export_bucket` / `bifrost_export_prefix` — where the export writes.
- `bifrost_lookback_days` — how many days back to re-load. Default 7.

### Params:
- `full_reload` — load every marked day the export holds, rather than only the window. For a
  backfill or the monthly re-observation.
- `lookback_days` — overrides the Variable for one run.
- `dry_run` — log the day plan and touch nothing.

The DAG deploys `is_paused_upon_creation=True`. Unpausing is the owner-gated activation step,
and it cannot be activated before the export exists.
"""

import logging
from datetime import UTC
from datetime import datetime as dt

from airflow.sdk import Param, Variable, dag, get_current_context, task
from include.custom_functions.bifrost_export import (
    days_in_window,
    ensure_landing_table,
    insert_day,
    success_marker_path,
)
from include.custom_functions.databricks_utils import connect_from_conn_id
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

LOOKBACK_VARIABLE = "bifrost_lookback_days"
DEFAULT_LOOKBACK_DAYS = 7


def _s3_client():
    """An S3 client for the export bucket.

    Credentials come from the worker's own identity rather than a named connection: the bucket
    belongs to the workbench account and is read through a role this deployment assumes, and an
    Airflow AWS *connection* would not help anyway. Reading the *objects* happens inside
    Databricks via `read_files`, which needs the Unity Catalog storage credential, not this
    client. This client only lists partitions and checks completion markers.
    """
    import boto3

    return boto3.client("s3")


def discover_complete_days(s3_client, bucket: str, prefix: str) -> list[tuple[str, str]]:
    """Every complete day partition in the export, as `(day, s3_prefix)`.

    A day counts as complete only when its `_SUCCESS` marker exists. The export writes whole
    days, so a day mid-write has data objects and no marker; loading it would land a partial
    day that no downstream test can detect.
    """
    base = f"{prefix.strip('/')}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    complete: dict[str, str] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=base, Delimiter="/"):
        for entry in page.get("CommonPrefixes", []):
            day_prefix = entry["Prefix"]
            token = day_prefix[len(base) :].strip("/")
            if not token.startswith("dt="):
                continue
            day = token[len("dt=") :]
            marker = success_marker_path(bucket, prefix, day)
            try:
                s3_client.head_object(Bucket=bucket, Key=marker.removeprefix(f"s3://{bucket}/"))
            except Exception:
                continue
            complete[day] = day_prefix
    return sorted(complete.items())


@dag(
    dag_id="load_bifrost_usage",
    start_date=datetime(2026, 11, 1, tz="UTC"),
    schedule="30 10 * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-engineering",
        # The export lands about 09:00 UTC and this follows it at 10:30, still hours clear of
        # the 00:02 UTC dbt build (which is 4pm Pacific; 10:30 UTC is 2:30-3:30am Pacific).
        "retries": 2,
        "retry_delay": duration(minutes=15),
    },
    doc_md=__doc__,
    tags=["bifrost", "finance"],
    params={
        "full_reload": Param(False, type="boolean", description="Load every day the export holds."),
        "lookback_days": Param(None, type=["null", "integer"], description="Override the lookback Variable."),
        "dry_run": Param(False, type="boolean", description="Log the plan and touch nothing."),
    },
)
def load_bifrost_usage():
    @task
    def list_days() -> list[dict]:
        context = get_current_context()
        params = context["params"]
        bucket = Variable.get("bifrost_export_bucket").strip()
        prefix = Variable.get("bifrost_export_prefix").strip()

        lookback = params.get("lookback_days") or int(
            Variable.get(LOOKBACK_VARIABLE, default=DEFAULT_LOOKBACK_DAYS)
        )
        today = dt.now(UTC).date().isoformat()

        days = days_in_window(
            [day for day, _ in discover_complete_days(_s3_client(), bucket, prefix)],
            lookback_days=lookback,
            today=today,
            full_reload=bool(params.get("full_reload")),
        )
        t_log.info("bifrost: %d complete day(s) in the window: %s", len(days), days)

        if params.get("dry_run"):
            return []
        return [{"day": day, "bucket": bucket, "prefix": prefix} for day in days]

    @task
    def load_day(plan: dict) -> int:
        catalog = Variable.get("databricks_catalog").strip()
        schema = Variable.get("bifrost_landing_schema").strip()

        with connect_from_conn_id(Variable.get("databricks_conn_id")) as connection:
            ensure_landing_table(connection, catalog, schema)
            return insert_day(
                connection,
                catalog=catalog,
                schema=schema,
                bucket=plan["bucket"],
                prefix=plan["prefix"],
                partition_day=plan["day"],
            )

    @task(trigger_rule="all_done")
    def summarize(inserted: list[int]) -> None:
        total = sum(value or 0 for value in inserted)
        t_log.info("bifrost: %d new row(s) across %d day(s)", total, len(inserted))
        if total == 0:
            t_log.info(
                "bifrost: no new rows. Expected on a re-run or a quiet day, since the INSERT "
                "skips rows already landed with the same id, status and cost."
            )

    days = list_days()
    summarize(load_day.expand(plan=days))


load_bifrost_usage()
