"""People-API voter refresh.

Thin sequencer over the loader CLI (installed on the worker image). Each step is a
BashOperator running `loader <step> --date {{ ds_nodash }}`; parallelism lives in the
loader, and inter-step state flows through the loader's S3 manifests (no Airflow xcom).

Postgres is reached via the gp_bastion_host SSH tunnel — the loader's LOADER_BASTION_*
env vars are populated from the gp_bastion_host connection. The unload step's Databricks
credentials (DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET, OAuth M2M) come
from the same Databricks connection the L2 dbt jobs use, chosen at runtime from the shared
`databricks_conn_id` Airflow Variable. The voter-data gate runs in dbt Cloud (which already has the
full monorepo project environment configured) rather than as a local dbt run. All connection- and
Variable-derived values are Jinja templates resolved at task runtime, so DAG parsing never touches
the metastore.
"""

from __future__ import annotations

from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from pendulum import datetime as pendulum_datetime
from pendulum import duration

# Databricks creds for the unload step (databricks-sdk, OAuth M2M), reused from the SAME connection
# the L2 dbt jobs use. WHICH connection is chosen at task RUNTIME from the shared `databricks_conn_id`
# Airflow Variable (per-deployment overridable: databricks_dev on dev, databricks on prod), NOT from a
# parse-time env var — Astro does not expose deployment env vars to the DAG processor at parse.
# `var.value.get(...)` reads the Variable and `conn.get(...)` fetches the connection, both at runtime;
# login/password is the standard SP-OAuth storage, with an extra_dejson fallback.
_DBX_CONN_EXPR = "conn.get(var.value.get('databricks_conn_id', 'databricks'))"
_DBX_ENV: dict[str, str] = {
    "DATABRICKS_HOST": "{% set c = " + _DBX_CONN_EXPR + " %}{{ c.host }}",
    "DATABRICKS_CLIENT_ID": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.login or c.extra_dejson.get('client_id', '') }}",
    "DATABRICKS_CLIENT_SECRET": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.password or c.extra_dejson.get('client_secret', '') }}",
    # unload's Statement Execution API is warehouse-only; take the warehouse id from the same
    # connection's http_path (`/sql/1.0/warehouses/<id>` -> last segment). The connection points at
    # a SQL warehouse (same one the L2 SQL jobs use), so creds AND warehouse come from one source —
    # no separate LOADER_DATABRICKS_WAREHOUSE_ID env var.
    "LOADER_DATABRICKS_WAREHOUSE_ID": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.extra_dejson.get('http_path', '').split('/') | last }}",
}

# Bastion fields for the loader's SSH tunnel, sourced from the gp_bastion_host connection.
# These are Jinja templates resolved at task runtime (not DAG parse — parse must not touch the
# metastore). The rest of the loader's config (LOADER_S3_BUCKET, LOADER_S3_IMPORT_ROLE_ARN,
# LOADER_AWS_ACCOUNT_ID, VPC/subnet/SG/KMS) is set as deployment env vars in the Astro Environment
# Manager and reaches the `loader` subprocess via append_env=True. Static, built once at import.
_LOADER_ENV: dict[str, str] = {
    "LOADER_BASTION_HOST": "{{ conn.gp_bastion_host.host }}",
    "LOADER_BASTION_PORT": "{{ conn.gp_bastion_host.port or 22 }}",
    "LOADER_BASTION_USER": "{{ conn.gp_bastion_host.login }}",
    "LOADER_BASTION_PRIVATE_KEY": "{{ conn.gp_bastion_host.extra_dejson.get('private_key', '') }}",
    "LOADER_BASTION_KEY_PASSPHRASE": "{{ conn.gp_bastion_host.extra_dejson.get('private_key_passphrase', '') }}",
}


# copy opens one SSH tunnel per file, so its --parallelism is also the count of concurrent SSH
# handshakes against the bastion. The loader default (128) overruns the bastion sshd's MaxStartups
# (default 10:30:100), which resets most connections ("Error reading SSH protocol banner"). Cap it to
# a value that stays under MaxStartups' unauthenticated-connection start threshold. Follow-up: pool
# and reuse a small number of tunnels in the loader so throughput isn't bounded by this.
_COPY_PARALLELISM = 8


def _step(
    task_id: str,
    subcommand: str,
    *,
    extra_env: dict[str, str] | None = None,
    extra_args: str = "",
) -> BashOperator:
    args = f" {extra_args}" if extra_args else ""
    return BashOperator(
        task_id=task_id,
        bash_command=f"loader {subcommand} --date {{{{ ds_nodash }}}}{args}",
        env={**_LOADER_ENV, **(extra_env or {})},
        append_env=True,
    )


@dag(
    dag_id="load_people_api",
    schedule="@monthly",
    start_date=pendulum_datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    # Created paused (like the other prod DAGs) so a fresh deploy doesn't auto-fire the current
    # monthly interval — catchup=False only suppresses historical backfill, not the current period.
    is_paused_upon_creation=True,
    default_args={"retries": 3, "retry_delay": duration(minutes=5)},
    tags=["people-api", "loader"],
)
def load_people_api():
    inspect_prod = _step("inspect_prod", "inspect-prod")
    # Voter-data quality gate: run the voter mart's dbt tests in dbt Cloud, which already has the full
    # monorepo project environment configured (running the project locally would need ~24 parse-time
    # DBT_* env vars, several secret). Reuses the shared dbt_cloud connection + dbt_cloud_job_id
    # Variable (per-deployment overridable), resolved at runtime; steps_override runs only this test.
    dbt_test_voter_gate = DbtCloudRunJobOperator(
        task_id="dbt_test_voter_gate",
        dbt_cloud_conn_id="dbt_cloud",
        job_id="{{ var.value.dbt_cloud_job_id }}",
        steps_override=["dbt test --select m_people_api__voter"],
        check_interval=30,
        timeout=1800,
    )
    unload = _step("unload", "unload", extra_env=_DBX_ENV)  # only loader step that reaches Databricks
    provision = _step("provision", "provision")
    create_schema = _step("create_schema", "create-schema")
    copy = _step("copy", "copy", extra_args=f"--parallelism {_COPY_PARALLELISM}")
    build_indexes = _step("build_indexes", "build-indexes")
    resize = _step("resize", "resize")
    validate = _step("validate", "validate")

    # dbt gate must pass before unload; unload + provision then run in parallel and both feed
    # create-schema; then the serial load chain.
    inspect_prod >> dbt_test_voter_gate >> [unload, provision]
    [unload, provision] >> create_schema
    create_schema >> copy >> build_indexes >> resize >> validate


load_people_api()
