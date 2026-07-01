"""People-API voter refresh.

Thin sequencer over the loader CLI (installed on the worker image). Each step is a
BashOperator running `loader <step> --date {{ ds_nodash }}`; parallelism lives in the
loader, and inter-step state flows through the loader's S3 manifests (no Airflow xcom).

Postgres is reached via the gp_bastion_host SSH tunnel — the loader's LOADER_BASTION_*
env vars are populated from the gp_bastion_host connection. The Databricks credentials
(DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET, OAuth M2M) come from a
Databricks Airflow connection (LOADER_DATABRICKS_CONN_ID, e.g. databricks_dev), feeding both
the loader's databricks-sdk and the Cosmos dbt gate from one source. All connection-derived
values are Jinja templates resolved at task runtime, so DAG parsing never touches the metastore.
"""

from __future__ import annotations

import os

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from cosmos import ProfileConfig
from cosmos.operators.virtualenv import DbtTestVirtualenvOperator
from pendulum import datetime as pendulum_datetime
from pendulum import duration

# Databricks creds + compute, reused from the SAME Databricks Airflow connection the L2 dbt jobs
# use (OAuth M2M). The conn_id is read at import (env var, parse-safe — Airflow 3 forbids metastore
# access at DAG parse) and interpolated into Jinja templates that resolve at task runtime.
# login/password is the standard SP-OAuth storage; the extra_dejson fallback covers connections that
# stash client_id/client_secret (and http_path) in extras. Applied ONLY to the steps that reach
# Databricks (the unload step + the Cosmos gate) — the other steps are AWS/Postgres only and must
# not depend on this connection existing.
_DBX_CONN = os.getenv("LOADER_DATABRICKS_CONN_ID", "databricks")
_DBX_ENV: dict[str, str] = {
    "DATABRICKS_HOST": f"{{{{ conn.{_DBX_CONN}.host }}}}",
    "DATABRICKS_CLIENT_ID": f"{{{{ conn.{_DBX_CONN}.login or conn.{_DBX_CONN}.extra_dejson.get('client_id', '') }}}}",
    "DATABRICKS_CLIENT_SECRET": f"{{{{ conn.{_DBX_CONN}.password or conn.{_DBX_CONN}.extra_dejson.get('client_secret', '') }}}}",
    # The gate's dbt profile reads this as its http_path, so the gate runs on the connection's own
    # compute (the L2 all-purpose cluster today) — no separate warehouse id needed for the gate.
    "DATABRICKS_HTTP_PATH": f"{{{{ conn.{_DBX_CONN}.extra_dejson.get('http_path', '') }}}}",
}

# Bastion fields for the loader's SSH tunnel, sourced from the gp_bastion_host connection.
# These are Jinja templates resolved at task runtime (not DAG parse — parse must not touch the
# metastore). The rest of the loader's config (LOADER_S3_BUCKET, LOADER_S3_IMPORT_ROLE_ARN,
# LOADER_AWS_ACCOUNT_ID, LOADER_DATABRICKS_WAREHOUSE_ID, VPC/subnet/SG/KMS) is set as deployment
# env vars in the Astro Environment Manager and reaches the `loader` subprocess via
# append_env=True. Static, so it's built once at import.
_LOADER_ENV: dict[str, str] = {
    "LOADER_BASTION_HOST": "{{ conn.gp_bastion_host.host }}",
    "LOADER_BASTION_PORT": "{{ conn.gp_bastion_host.port or 22 }}",
    "LOADER_BASTION_USER": "{{ conn.gp_bastion_host.login }}",
    "LOADER_BASTION_PRIVATE_KEY": "{{ conn.gp_bastion_host.extra_dejson.get('private_key', '') }}",
    "LOADER_BASTION_KEY_PASSPHRASE": "{{ conn.gp_bastion_host.extra_dejson.get('private_key_passphrase', '') }}",
}


def _step(task_id: str, subcommand: str, *, extra_env: dict[str, str] | None = None) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"loader {subcommand} --date {{{{ ds_nodash }}}}",
        env={**_LOADER_ENV, **(extra_env or {})},
        append_env=True,
    )


# dbt project + profile shipped in the Astro image; override via env if placed elsewhere.
_DBT_PROJECT_DIR = os.getenv("LOADER_DBT_PROJECT_DIR", "/usr/local/airflow/dbt/project")
_DBT_PROFILES_YML = os.getenv("LOADER_DBT_PROFILES_YML", "/usr/local/airflow/include/loader_dbt_profiles.yml")


def _voter_gate_profile() -> ProfileConfig:
    """dbt profile for the voter gate.

    Uses a committed profiles.yml that authenticates via OAuth M2M from the DATABRICKS_* env vars.
    Those are injected into the operator's env (see _DBX_ENV) from the same Databricks connection
    the loader's databricks-sdk uses — one OAuth credential across every process. dbt resolves the
    env vars (host, client id/secret, warehouse http_path, catalog, schema) at task runtime.
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
    tags=["people-api", "loader"],
)
def load_people_api():
    inspect_prod = _step("inspect_prod", "inspect-prod")
    dbt_test_voter_gate = DbtTestVirtualenvOperator(
        task_id="dbt_test_voter_gate",
        project_dir=_DBT_PROJECT_DIR,
        profile_config=_voter_gate_profile(),
        select=["m_people_api__voter"],  # voter schema tests + the singular gate test
        install_deps=True,  # the dbt project has packages.yml; run `dbt deps` before testing
        # Databricks creds from the same connection as the loader (templated, resolved at runtime).
        # append_env=True keeps the deployment env vars the profile also needs
        # (LOADER_DATABRICKS_WAREHOUSE_ID, LOADER_DBT_CATALOG, LOADER_DBT_SCHEMA).
        env=_DBX_ENV,
        append_env=True,
        # Run dbt in an isolated venv: dbt-databricks pins databricks-sdk <0.105, which conflicts
        # with the image's databricks-sdk >=0.117. py_system_site_packages stays False (default)
        # so the venv is fully isolated from the image's SDK.
        py_requirements=["dbt-databricks>=1.8,<2.0"],
    )
    unload = _step("unload", "unload", extra_env=_DBX_ENV)  # only step that reaches Databricks
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
