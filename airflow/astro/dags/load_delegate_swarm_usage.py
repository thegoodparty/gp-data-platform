"""## Load the delegate swarm's usage ledger into the warehouse

Two jobs, and this DAG is the second one. A systemd timer on the swarm host
(`thegoodparty/ops` owns it) reads the swarm's own SQLite ledger and writes two JSON Lines files per
run to two S3 prefixes; this DAG lands them in `airflow_source.delegate_swarm_session_cost` and
`airflow_source.delegate_swarm_task_edge`, which dbt then models into `mart_finance`. A sibling of
`load_bifrost_usage`: the gateway export and this one differ in the file format and in having two
files rather than one, and in nothing else that matters here.

### Why the swarm needs its own export

The swarm calls Fireworks directly through pi's own provider id and never reaches the Bifrost
gateway, so its spend cannot be reconstructed from `bifrost_request_log` at any key. Its ledger is
upstream `agent-swarm`'s `session_costs` table, inside a Docker volume on the swarm host's EBS data
volume with no published port, which is why the exporter is a timer on that host and not a Lambda.

### One task, because COPY INTO keeps the bookkeeping

`COPY INTO` is idempotent: Databricks documents that files already loaded are skipped on subsequent
runs, and that this holds even if a file was modified since. So the DAG names the two prefixes and
lets the warehouse work out what is new. There is no listing task, no per-day mapped task, no
watermark and no anti-join. A re-run loads nothing, and a new export's file is the only thing picked
up.

### Airflow holds no AWS credentials

The export never rewrites a file: each run writes one new object per table whose name carries the run
time, so a file is complete or absent and there is no marker to poll for. Nothing here lists or reads
S3, which means no grant on the engineer's bucket for Astro. Databricks reads each prefix through its
own Unity Catalog storage credential, and that is the one reader.

### Two files, so a run can land one and not the other

The two objects are written in a fixed order (cost first) and a failure stops the run between them.
So a cost file can land while its edge file does not, which mis-roots that day's tasks until the next
run's 7 day window brings the edges in, and heals with no help from here. Both tables are created
before either is loaded, so a landing table that does not exist fails the run before a file is
touched.

### Nothing is deleted

Both landing tables are append-only, like every other job's record here. There is no prune task,
because deleting an observation would make the mart's history for that day diverge the next time it
rebuilds. A run appends a whole window (7 days daily, 90 days monthly), staging keeps the newest
`exported_at` per key, and the bucket keeps every file, so any day can be reproduced as of any run.

### Connections (set in Astro Environment Manager):
- `databricks_conn_id` (Variable) names a Databricks connection (Generic, OAuth M2M).

### Variables:
- `databricks_conn_id` — the Variable naming the Databricks connection.
- `databricks_catalog` — the Unity Catalog catalog.
- `delegate_swarm_landing_schema` — the ingest schema this DAG writes (`airflow_source` in prod,
  `airflow_source_dev` in dev). There is one catalog, so the schema is what keeps a dev run off
  production data.
- `delegate_swarm_export_bucket` / `delegate_swarm_cost_prefix` / `delegate_swarm_task_edge_prefix` —
  where the export writes. Two prefixes, one per table, because `COPY INTO` reads a whole prefix: a
  single shared one would make each table's load read the other table's file and fail on the columns
  it does not have.

### Params:
- `dry_run` — build and log both statements, run nothing.

The DAG deploys `is_paused_upon_creation=True`. Unpausing is the owner-gated activation step, and it
cannot be activated before the export exists and before the Unity Catalog external location for
`s3://<bucket>/delegate-swarm` is granted.
"""

import logging

from airflow.sdk import Param, Variable, dag, get_current_context, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.delegate_swarm_export import (
    SESSION_COST,
    TASK_EDGE,
    Export,
    build_copy_sql,
    export_prefix,
    land_exports,
)
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")


@dag(
    dag_id="load_delegate_swarm_usage",
    start_date=datetime(2026, 11, 1, tz="UTC"),
    schedule="45 10 * * *",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={
        "owner": "data-engineering",
        # The export runs at 09:15 UTC with a 30 minute bound, so it is done by 09:45 and this
        # follows at 10:45, clear of the gateway loader at 10:30 and the 10:00 data volume snapshot.
        "retries": 2,
        "retry_delay": duration(minutes=15),
    },
    doc_md=__doc__,
    tags=["delegate-swarm", "finance"],
    params={
        "dry_run": Param(False, type="boolean", description="Log the statements and run nothing."),
    },
)
def load_delegate_swarm_usage():
    @task
    def load() -> dict[str, int]:
        catalog = Variable.get("databricks_catalog").strip()
        schema = Variable.get("delegate_swarm_landing_schema").strip()
        bucket = Variable.get("delegate_swarm_export_bucket").strip()
        exports = (
            Export(table=SESSION_COST, prefix=Variable.get("delegate_swarm_cost_prefix").strip()),
            Export(table=TASK_EDGE, prefix=Variable.get("delegate_swarm_task_edge_prefix").strip()),
        )

        if get_current_context()["params"].get("dry_run"):
            for export in exports:
                t_log.info(
                    "delegate swarm dry run (%s): %s",
                    export.table.name,
                    build_copy_sql(catalog, schema, export_prefix(bucket, export.prefix), export.table),
                )
            return {}

        # `connect_from_conn_id` resolves the `databricks_conn_id` Variable itself; passing it a
        # resolved value makes it treat a connection id as another Variable name.
        with connect_from_conn_id() as connection:
            return land_exports(connection, catalog, schema, bucket, exports)

    @task
    def report(loaded: dict[str, int]) -> None:
        for table_name, count in loaded.items():
            t_log.info("delegate swarm: %d new row(s) landed in %s", count, table_name)
            if count == 0:
                t_log.info(
                    "delegate swarm: no new rows in %s. Expected on a re-run or before the export "
                    "has written a new file, since COPY INTO skips files it has already loaded.",
                    table_name,
                )

    report(load())


load_delegate_swarm_usage()
