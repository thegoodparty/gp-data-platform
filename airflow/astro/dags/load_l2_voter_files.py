"""
## Load L2 Voter Files

Mirrors L2's files from SFTP into S3, then rebuilds the `l2_s3_*` source tables the dbt staging
layer reads: the per-state VM2 and VM2Uniform archives plus the expired-ID file. Replaces the
`load__l2_sftp_to_s3` and `load__l2_s3_to_databricks` dbt models and the `l2_expired_voters` DAG.

1. **plan_sources** — the files L2 publishes, minus those already in S3
2. **sync** (one task per file) — download it, upload it or the members we keep
3. **plan_table_loads** — the newest staged file per table, minus the tables already built from
   it. Runs whatever the sync did, so a file staged earlier but never loaded is picked up here.
4. **load** (one task per table) — `CREATE OR REPLACE TABLE ... AS SELECT` over `read_files`

Both plans read live state, so a run that dies part way is retried by the next one and a night
with no new files does nothing.

### Configuration

**Connections:**
- `l2_sftp` (SFTP)
- `aws_default` (Amazon Web Services) — `s3:ListBucket` and `s3:PutObject` on the staging prefix.
  Leave the credentials empty to use the worker's own credential chain.
- `databricks` / `databricks_dev` (Generic) — host, login (OAuth client_id), password (OAuth
  client_secret), extras: `{"http_path": "/sql/1.0/warehouses/..."}`

**Variables:**
- `l2_s3_bucket`
- `l2_voter_files_s3_prefix` — e.g. `l2_data/from_sftp_server/VMFiles/prod`
- `l2_voter_files_databricks_schema` — e.g. `dbt_source`
- `l2_sftp_expired_dir`, `l2_sftp_expired_file_pattern` — where the expired-ID file lives
- `databricks_conn_id`

The SQL warehouse reads the staged files through Unity Catalog, so the staging prefix needs an
external location granted `READ FILES` to the Airflow service principal.

**Params:**
- `dry_run` — log both plans without copying or loading.

The sync task downloads a whole archive before uploading it. Astro workers have a fixed 10 GiB of
ephemeral storage that no worker type or queue setting can raise, and the largest archive is ~8 GB
compressed, so `SYNC_QUEUE` must be a queue with a concurrency of 1.
"""

import logging
from tempfile import TemporaryDirectory

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import BaseHook, Param, Variable, dag, get_current_context, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.l2_sftp import create_sftp_connection
from include.custom_functions.l2_voter_loaders import (
    create_schema,
    get_table_loaded_at,
    list_remote_sources,
    list_s3_objects,
    load_table,
    plan_loads,
    plan_transfers,
    sync_source,
)
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

DATABRICKS_CATALOG = "goodparty_data_catalog"
AWS_CONN_ID = "aws_default"
# Dedicated worker queue with a concurrency of 1: a worker's 10 GiB of ephemeral storage fits one
# archive, so a second concurrent download on the same worker would exhaust it.
SYNC_QUEUE = "l2-voter-files"


def _sftp_connection():
    conn = BaseHook.get_connection("l2_sftp")
    return create_sftp_connection(
        host=conn.host, port=conn.port or 22, username=conn.login, password=conn.password
    )


def _s3_client():
    return S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()


def _is_dry_run() -> bool:
    return bool(get_current_context()["params"]["dry_run"])


@dag(
    start_date=datetime(2026, 8, 14),
    schedule="0 8 * * *",
    max_active_runs=1,
    doc_md=__doc__,
    catchup=False,
    default_args={
        "owner": "Data Engineering Team",
        "retries": 2,
        "retry_delay": duration(minutes=5),
    },
    params={"dry_run": Param(False, type="boolean", description="Log both plans without doing work")},
    is_paused_upon_creation=True,
)
def load_l2_voter_files():
    @task
    def plan_sources() -> list[dict]:
        """The files L2 publishes that S3 does not already hold."""
        prefix = Variable.get("l2_voter_files_s3_prefix")

        transport, sftp_client = _sftp_connection()
        try:
            sources = list_remote_sources(
                sftp_client,
                expired_dir=Variable.get("l2_sftp_expired_dir"),
                expired_pattern=Variable.get("l2_sftp_expired_file_pattern"),
            )
        finally:
            sftp_client.close()
            transport.close()

        staged = list_s3_objects(_s3_client(), Variable.get("l2_s3_bucket"), prefix)
        pending = plan_transfers(sources, staged)

        t_log.info(f"L2 publishes {len(sources)} file(s); {len(pending)} to copy")
        for source in pending:
            t_log.info(f"  {source['remote_path']} ({source['size_bytes'] / 1024**3:.2f} GB)")

        return [] if _is_dry_run() else pending

    # max_active_tis_per_dag keeps the one-archive-per-worker invariant in code, where it is
    # reviewable, rather than resting on the queue's concurrency setting alone.
    @task(queue=SYNC_QUEUE, max_active_tis_per_dag=1, execution_timeout=duration(hours=6))
    def sync(source: dict) -> list[str]:
        """Copy one source file into S3."""
        transport, sftp_client = _sftp_connection()
        try:
            with TemporaryDirectory(prefix="l2_voter_files_") as staging_dir:
                return sync_source(
                    sftp_client=sftp_client,
                    s3_client=_s3_client(),
                    bucket=Variable.get("l2_s3_bucket"),
                    prefix=Variable.get("l2_voter_files_s3_prefix"),
                    source=source,
                    staging_dir=staging_dir,
                )
        finally:
            sftp_client.close()
            transport.close()

    # all_done: the Databricks side reads S3, not the sync results, so one state failing to copy
    # must not stop the states that did.
    @task(trigger_rule="all_done")
    def plan_table_loads() -> list[dict]:
        """The staged files newer than the tables built from them."""
        schema = Variable.get("l2_voter_files_databricks_schema")
        staged = list_s3_objects(
            _s3_client(), Variable.get("l2_s3_bucket"), Variable.get("l2_voter_files_s3_prefix")
        )

        connection = connect_from_conn_id()
        try:
            if not _is_dry_run():
                # Once per run, rather than ahead of every mapped load.
                create_schema(connection, DATABRICKS_CATALOG, schema)
            loaded_at = get_table_loaded_at(connection, DATABRICKS_CATALOG, schema)
        finally:
            connection.close()

        pending = plan_loads(staged, loaded_at)

        t_log.info(f"{len(pending)} table(s) to rebuild in {DATABRICKS_CATALOG}.{schema}")
        for load_spec in pending:
            t_log.info(f"  {load_spec['table_name']} <- {load_spec['source_file_name']}")

        return [] if _is_dry_run() else pending

    @task(max_active_tis_per_dag=8, execution_timeout=duration(hours=6))
    def load(table_load: dict) -> str:
        """Rebuild one `l2_s3_*` table from its staged file."""
        connection = connect_from_conn_id()
        try:
            return load_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=Variable.get("l2_voter_files_databricks_schema"),
                bucket=Variable.get("l2_s3_bucket"),
                prefix=Variable.get("l2_voter_files_s3_prefix"),
                load=table_load,
            )
        finally:
            connection.close()

    synced = sync.expand(source=plan_sources())
    table_loads = plan_table_loads()
    synced >> table_loads
    load.expand(table_load=table_loads)


load_l2_voter_files()
