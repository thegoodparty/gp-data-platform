"""
## Load L2 Voter Files

Mirrors L2's files from SFTP into S3, then rebuilds the `l2_s3_*` source tables the dbt staging
layer reads: the per-state VM2, VM2Uniform and Haystaq issue-model archives plus the expired-ID
file. Replaces the four `load__l2_*` dbt models and the `l2_expired_voters` DAG.

1. **plan_sources** — the files L2 publishes, minus those already in S3
2. **sync** (one task per file) — download it, upload it or the members we keep
3. **plan_table_loads** — the newest staged file per table, minus the tables already built from
   it. Runs whatever the sync did, so a file staged earlier but never loaded is picked up here.
4. **load** (one task per table) — `CREATE OR REPLACE TABLE ... AS SELECT` over `read_files`

Both plans read live state, so a run that dies part way is retried by the next one and a night
with no new files does nothing.

Which files exist is declared in one place: `SOURCE_GROUPS` in `l2_voter_loaders`. A new feed on
this server needs an entry there and nothing here.

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

**Params:** both scope the sync only. The table plan is driven by what S3 holds rather than by
this run, so it is unaffected by either.

- `dry_run` — log both plans and touch nothing. It does not read Databricks either, so the
  table plan it prints is an upper bound: every staged file looks unloaded.
- `groups` — sync only these `SOURCE_GROUPS` names, and/or `EXPIRED`. Empty means all.
- `folders` — sync only these state codes, and/or `EXPIRED`. Empty means all.

The sync task downloads a whole archive before uploading it. Astro workers have a fixed 10 GiB of
ephemeral storage that no worker type or queue setting can raise, and the largest archive is ~7 GB
compressed, so `SYNC_QUEUE` must be a queue with a concurrency of 1.
"""

import logging
from tempfile import TemporaryDirectory

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import BaseHook, Param, Variable, dag, get_current_context, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.l2_voter_loaders import (
    SOURCE_GROUPS,
    create_schema,
    get_table_loaded_at,
    list_remote_sources,
    list_s3_objects,
    load_table,
    plan_loads,
    plan_transfers,
    sync_source,
)
from include.custom_functions.sftp_utils import create_sftp_connection
from pendulum import datetime, duration

t_log = logging.getLogger("airflow.task")

DATABRICKS_CATALOG = "goodparty_data_catalog"
AWS_CONN_ID = "aws_default"
SYNC_QUEUE = "l2-voter-files"


def _sftp_connection():
    conn = BaseHook.get_connection("l2_sftp")
    return create_sftp_connection(
        host=conn.host, port=conn.port or 22, username=conn.login, password=conn.password
    )


def _s3_client():
    return S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()


def _staging_location() -> tuple[str, str]:
    return Variable.get("l2_s3_bucket"), Variable.get("l2_voter_files_s3_prefix")


def _staged_objects() -> dict:
    return list_s3_objects(_s3_client(), *_staging_location())


def _param(name: str):
    return get_current_context()["params"][name]


def _scoped(sources: list[dict], param: str, key: str) -> list[dict]:
    """Narrow the sync to the values `param` names. Empty means all of them."""
    wanted = set(_param(param))
    if not wanted:
        return sources
    t_log.info(f"Limiting the sync to {param} {sorted(wanted)}")
    return [source for source in sources if source[key] in wanted]


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
            description=f"Sync only these groups — {', '.join(SOURCE_GROUPS)}, EXPIRED. Empty means all.",
        ),
        "folders": Param(
            [],
            type="array",
            items={"type": "string"},
            description="Sync only these folders — state codes and/or EXPIRED. Empty means all.",
        ),
    },
    is_paused_upon_creation=True,
)
def load_l2_voter_files():
    @task
    def plan_sources() -> list[dict]:
        """The files L2 publishes that S3 does not already hold."""
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

        sources = _scoped(_scoped(sources, "groups", "group"), "folders", "folder")
        pending = plan_transfers(sources, _staged_objects())

        t_log.info(f"L2 publishes {len(sources)} file(s); {len(pending)} to copy")
        for source in pending:
            t_log.info(f"  {source['remote_path']} ({source['size_bytes'] / 1024**3:.2f} GB)")

        return [] if _param("dry_run") else pending

    # max_active_tis_per_dag keeps one archive per worker even if the queue is reconfigured.
    @task(queue=SYNC_QUEUE, max_active_tis_per_dag=1, execution_timeout=duration(hours=6))
    def sync(source: dict) -> list[str]:
        """Copy one source file into S3."""
        bucket, prefix = _staging_location()
        transport, sftp_client = _sftp_connection()
        try:
            with TemporaryDirectory(prefix="l2_voter_files_") as staging_dir:
                return sync_source(
                    sftp_client=sftp_client,
                    s3_client=_s3_client(),
                    bucket=bucket,
                    prefix=prefix,
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
        staged = _staged_objects()

        dry_run = _param("dry_run")
        if dry_run:
            # Keep a dry run usable where Databricks is unreachable. Without the table
            # timestamps every staged file looks unloaded, so this is an upper bound.
            t_log.info("dry_run: skipping the Databricks read; the plan below is an upper bound")
            loaded_at: dict = {}
        else:
            connection = connect_from_conn_id()
            try:
                # Once per run, rather than ahead of every mapped load.
                create_schema(connection, DATABRICKS_CATALOG, schema)
                loaded_at = get_table_loaded_at(connection, DATABRICKS_CATALOG, schema)
            finally:
                connection.close()

        pending = plan_loads(staged, loaded_at)

        t_log.info(f"{len(pending)} table(s) to rebuild in {DATABRICKS_CATALOG}.{schema}")
        for load_spec in pending:
            t_log.info(f"  {load_spec['table_name']} <- {load_spec['source_file_name']}")

        return [] if dry_run else pending

    @task(max_active_tis_per_dag=8, execution_timeout=duration(hours=6))
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

    synced = sync.expand(source=plan_sources())
    table_loads = plan_table_loads()
    synced >> table_loads
    load.expand(table_load=table_loads)


load_l2_voter_files()
