"""
## Load L2 Voter Files

Mirrors L2's SFTP files into S3, then rebuilds the `l2_s3_*` tables the dbt staging layer reads:
the per-state VM2, VM2Uniform and Haystaq archives plus the nationwide expired-ID file. Replaces
the four `load__l2_*` dbt models and the `l2_expired_voters` DAG.

    plan_sources -> sync (one task per file) -> plan_table_loads -> load (one task per table)

Nothing is recorded between runs. Both plans come from live state — the SFTP listing against S3,
then S3 against each table's last-altered time — so a run that dies part way is finished by the
next one, and a night with no new files does nothing.

`SOURCE_GROUPS` in `l2_voter_loaders` is the only place that knows what L2 publishes. A new feed
on this server needs an entry there and nothing here.

### Configuration

Connections: `l2_sftp` (SFTP), `aws_default` (AWS, needing `s3:ListBucket` and `s3:PutObject` on
the staging prefix; leave its credentials empty to use the worker's own), and the Databricks
connection the `databricks_conn_id` variable names. The SQL warehouse reads the staged files
through Unity Catalog, so the staging prefix needs an external location granted `READ FILES` to
the Airflow service principal.

Variables: `l2_s3_bucket`, `l2_voter_files_s3_prefix`, `l2_voter_files_databricks_schema` (the
dbt sources read a fixed `dbt_source`, so in prod this must be that — pointing it elsewhere
stages a copy dbt will not see) and `databricks_conn_id`.

Params scope the sync only, since the table plan follows S3 rather than this run. `dry_run` logs
both plans and touches nothing, and skips reading Databricks, so the table plan it prints is an
upper bound. `groups` and `folders` narrow the sync to those names.

`sync` downloads a whole archive before uploading it, and Astro workers have a fixed 10 GiB of
ephemeral storage that no worker type or queue setting can raise. The largest archive is ~7 GB
compressed, so `SYNC_QUEUE` must be a queue with a concurrency of 1.
"""

import logging
from tempfile import TemporaryDirectory

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import BaseHook, Param, Variable, dag, get_current_context, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.l2_voter_loaders import (
    EXPIRED_FOLDER,
    SOURCE_GROUPS,
    create_schema,
    list_remote_sources,
    list_s3_objects,
    load_table,
    plan_loads,
    plan_transfers,
    sftp_session,
    sync_source,
    table_loaded_at,
)
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

DATABRICKS_CATALOG = "goodparty_data_catalog"
SYNC_QUEUE = "l2-voter-files"


def _sftp_session():
    conn = BaseHook.get_connection("l2_sftp")
    return sftp_session(host=conn.host, port=conn.port or 22, username=conn.login, password=conn.password)


def _s3_client():
    return S3Hook(aws_conn_id="aws_default").get_conn()


def _staging_location() -> tuple[str, str]:
    return Variable.get("l2_s3_bucket"), Variable.get("l2_voter_files_s3_prefix")


def _staged_objects() -> dict:
    return list_s3_objects(_s3_client(), *_staging_location())


def _param(name: str):
    return get_current_context()["params"][name]


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
    params={
        "dry_run": Param(False, type="boolean", description="Log both plans without doing work"),
        "groups": Param(
            [],
            type="array",
            items={"type": "string"},
            description=f"Sync only these groups — {', '.join(SOURCE_GROUPS)}, {EXPIRED_FOLDER}. Empty means all.",
        ),
        "folders": Param(
            [],
            type="array",
            items={"type": "string"},
            description=f"Sync only these folders — state codes and/or {EXPIRED_FOLDER}. Empty means all.",
        ),
    },
    is_paused_upon_creation=True,
)
def load_l2_voter_files():
    @task
    def plan_sources() -> list[dict]:
        """The files L2 publishes that S3 does not already hold."""
        with _sftp_session() as sftp_client:
            sources = list_remote_sources(sftp_client)

        for param, key in (("groups", "group"), ("folders", "folder")):
            if wanted := set(_param(param)):
                t_log.info(f"Limiting the sync to {param} {sorted(wanted)}")
                sources = [source for source in sources if source[key] in wanted]

        pending = plan_transfers(sources, _staged_objects())
        t_log.info(f"L2 publishes {len(sources)} file(s); {len(pending)} to copy")
        for source in pending:
            t_log.info(f"  {source['remote_path']} ({source['size_bytes'] / 1024**3:.2f} GB)")

        return [] if _param("dry_run") else pending

    # max_active_tis_per_dag keeps one archive per worker even if the queue is reconfigured.
    @task(
        queue=SYNC_QUEUE,
        max_active_tis_per_dag=1,
        execution_timeout=duration(hours=6),
        map_index_template="{{ task.op_kwargs['source']['remote_path'].split('/') | last }}",
    )
    def sync(source: dict) -> list[str]:
        """Copy one source file into S3."""
        bucket, prefix = _staging_location()
        with _sftp_session() as sftp_client, TemporaryDirectory(prefix="l2_voter_files_") as staging_dir:
            return sync_source(
                sftp_client=sftp_client,
                s3_client=_s3_client(),
                bucket=bucket,
                prefix=prefix,
                source=source,
                staging_dir=staging_dir,
            )

    # all_done: the Databricks side reads S3, not the sync results, so one state failing to copy
    # must not stop the states that did.
    @task(trigger_rule="all_done")
    def plan_table_loads() -> list[dict]:
        """The staged files newer than the tables built from them."""
        schema = Variable.get("l2_voter_files_databricks_schema")
        staged = _staged_objects()
        dry_run = _param("dry_run")

        loaded_at: dict = {}
        if not dry_run:
            connection = connect_from_conn_id()
            try:
                create_schema(connection, DATABRICKS_CATALOG, schema)
                loaded_at = table_loaded_at(connection, DATABRICKS_CATALOG, schema)
            finally:
                connection.close()

        pending = plan_loads(staged, loaded_at)
        t_log.info(f"{len(pending)} table(s) to rebuild in {DATABRICKS_CATALOG}.{schema}")
        for load_spec in pending:
            t_log.info(f"  {load_spec['table_name']} <- {load_spec['source_file_name']}")

        return [] if dry_run else pending

    @task(
        max_active_tis_per_dag=8,
        execution_timeout=duration(hours=6),
        map_index_template="{{ task.op_kwargs['table_load']['table_name'] }}",
    )
    def load(table_load: dict) -> str:
        """Rebuild one `l2_s3_*` table from its staged file."""
        bucket, prefix = _staging_location()
        connection = connect_from_conn_id()
        try:
            return load_table(
                connection=connection,
                catalog=DATABRICKS_CATALOG,
                schema=Variable.get("l2_voter_files_databricks_schema"),
                bucket=bucket,
                prefix=prefix,
                load=table_load,
            )
        finally:
            connection.close()

    table_loads = plan_table_loads()
    sync.expand(source=plan_sources()) >> table_loads
    load.expand(table_load=table_loads)


load_l2_voter_files()
