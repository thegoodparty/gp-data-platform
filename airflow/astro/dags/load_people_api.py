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
from pendulum import datetime as pendulum_datetime
from pendulum import duration

# Bastion fields for the loader's SSH tunnel, sourced from the gp_bastion_host connection.
# These are Jinja templates resolved at task runtime (not DAG parse — parse must not touch the
# metastore). The rest of the loader's config (LOADER_S3_BUCKET, LOADER_S3_IMPORT_ROLE_ARN,
# LOADER_AWS_ACCOUNT_ID, LOADER_DATABRICKS_WAREHOUSE_ID, VPC/subnet/SG/KMS) is set as deployment
# env vars in the Astro Environment Manager and reaches the `loader` subprocess via
# append_env=True — consistent with the loader's native env-var config and the Cosmos gate's
# os.getenv reads below. Static, so it's built once at import.
_LOADER_ENV: dict[str, str] = {
    "LOADER_BASTION_HOST": "{{ conn.gp_bastion_host.host }}",
    "LOADER_BASTION_PORT": "{{ conn.gp_bastion_host.port or 22 }}",
    "LOADER_BASTION_USER": "{{ conn.gp_bastion_host.login }}",
    "LOADER_BASTION_PRIVATE_KEY": "{{ conn.gp_bastion_host.extra_dejson.get('private_key', '') }}",
    "LOADER_BASTION_KEY_PASSPHRASE": "{{ conn.gp_bastion_host.extra_dejson.get('private_key_passphrase', '') }}",
}


def _step(task_id: str, subcommand: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"loader {subcommand} --date {{{{ ds_nodash }}}}",
        env=_LOADER_ENV,
        append_env=True,
    )


# dbt project + profile shipped in the Astro image; override via env if placed elsewhere.
_DBT_PROJECT_DIR = os.getenv("LOADER_DBT_PROJECT_DIR", "/usr/local/airflow/dbt/project")
_DBT_PROFILES_YML = os.getenv("LOADER_DBT_PROFILES_YML", "/usr/local/airflow/include/loader_dbt_profiles.yml")


def _voter_gate_profile() -> ProfileConfig:
    """dbt profile for the voter gate.

    Uses a committed profiles.yml that authenticates via OAuth M2M from the same env vars as the
    loader's databricks-sdk (DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET) —
    one OAuth credential (the people-api-loader SP) across every process, no Airflow connection.
    dbt resolves the env vars (host, warehouse http_path, schema) at task runtime.
    """
    return ProfileConfig(
        profile_name="default",
        target_name="loader",
        profiles_yml_filepath=_DBT_PROFILES_YML,
    )


@dag(
    dag_id="load_people_api",
    schedule="@monthly",
    start_date=pendulum_datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 3, "retry_delay": duration(minutes=5)},
    tags=["people-api", "loader", "DATA-1640"],
)
def load_people_api():
    inspect_prod = _step("inspect_prod", "inspect-prod")
    dbt_test_voter_gate = DbtTestLocalOperator(
        task_id="dbt_test_voter_gate",
        project_dir=_DBT_PROJECT_DIR,
        profile_config=_voter_gate_profile(),
        select=["m_people_api__voter"],  # voter schema tests + the DATA-1906 singular gate test
        install_deps=True,  # the dbt project has packages.yml; run `dbt deps` before testing
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


load_people_api()
