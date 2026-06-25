"""People-API voter refresh (DATA-1913).

Thin sequencer over the loader CLI (installed on the worker image). Each step is a
BashOperator running `loader <step> --date {{ ds_nodash }}`; parallelism lives in the
loader, and inter-step state flows through the loader's S3 manifests (no Airflow xcom).

Postgres is reached via the gp_bastion_host SSH tunnel — the loader's LOADER_BASTION_*
env vars (Plan A) are populated from the gp_bastion_host connection. All LOADER_* env
values are Jinja templates resolved at task runtime, so DAG parsing never touches the
metastore.
"""

from __future__ import annotations

import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from cosmos import ProfileConfig
from cosmos.operators.local import DbtTestLocalOperator
from cosmos.profiles import DatabricksOauthProfileMapping
from pendulum import datetime as pendulum_datetime
from pendulum import duration


def _loader_env() -> dict[str, str]:
    """Bastion fields for the loader's SSH tunnel, from the gp_bastion_host connection.

    Templated so they resolve at task runtime (not DAG parse — parse must not touch the
    metastore). The rest of the loader's config (LOADER_S3_BUCKET, LOADER_S3_IMPORT_ROLE_ARN,
    LOADER_AWS_ACCOUNT_ID, LOADER_DATABRICKS_WAREHOUSE_ID, VPC/subnet/SG/KMS) is set as
    deployment env vars in the Astro Environment Manager and reaches the `loader` subprocess
    via append_env=True — consistent with the loader's native env-var config and with the
    Cosmos gate's os.environ reads below.
    """
    return {
        "LOADER_BASTION_HOST": "{{ conn.gp_bastion_host.host }}",
        "LOADER_BASTION_PORT": "{{ conn.gp_bastion_host.port or 22 }}",
        "LOADER_BASTION_USER": "{{ conn.gp_bastion_host.login }}",
        "LOADER_BASTION_PRIVATE_KEY": "{{ conn.gp_bastion_host.extra_dejson.get('private_key', '') }}",
    }


def _step(task_id: str, subcommand: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"loader {subcommand} --date {{{{ ds_nodash }}}}",
        env=_loader_env(),
        append_env=True,
    )


# dbt project shipped in the Astro image; override via env if the build places it elsewhere.
_DBT_PROJECT_DIR = os.environ.get("LOADER_DBT_PROJECT_DIR", "/usr/local/airflow/dbt/project")


def _voter_gate_profile() -> ProfileConfig:
    """dbt profile for the voter gate, built from the `databricks` (OAuth M2M) connection.

    host / client_id / client_secret come from the connection; schema and http_path (the SQL
    warehouse) come from deployment env vars, read at parse time because the ProfileConfig is
    constructed during DAG parsing (Airflow Variables would require the metastore).
    """
    warehouse_id = os.environ.get("LOADER_DATABRICKS_WAREHOUSE_ID", "")
    return ProfileConfig(
        profile_name="default",
        target_name="loader",
        profile_mapping=DatabricksOauthProfileMapping(
            conn_id="databricks",
            profile_args={
                "schema": os.environ.get("LOADER_DBT_SCHEMA", "dbt"),
                "http_path": f"/sql/1.0/warehouses/{warehouse_id}",
            },
        ),
    )


@dag(
    dag_id="load_people_api_v2",
    schedule="@monthly",
    start_date=pendulum_datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": duration(minutes=5)},
    tags=["people-api", "loader", "DATA-1640"],
)
def load_people_api_v2():
    inspect_prod = _step("inspect_prod", "inspect-prod")
    dbt_test_voter_gate = DbtTestLocalOperator(
        task_id="dbt_test_voter_gate",
        project_dir=_DBT_PROJECT_DIR,
        profile_config=_voter_gate_profile(),
        select=["m_people_api__voter"],  # voter schema tests + the DATA-1906 singular gate test
    )
    unload = _step("unload", "unload")
    provision = _step("provision", "provision")
    create_schema = _step("create_schema", "create-schema")
    copy = _step("copy", "copy")
    build_indexes = _step("build_indexes", "build-indexes")
    resize = _step("resize", "resize")
    validate = _step("validate", "validate")

    # dbt gate must pass before unload; unload + provision then run in parallel and both feed
    # create-schema; then the serial load chain.
    inspect_prod >> dbt_test_voter_gate >> [unload, provision]
    [unload, provision] >> create_schema
    create_schema >> copy >> build_indexes >> resize >> validate


load_people_api_v2()
