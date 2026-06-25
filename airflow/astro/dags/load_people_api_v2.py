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

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from pendulum import datetime as pendulum_datetime
from pendulum import duration


def _loader_env() -> dict[str, str]:
    """Templated LOADER_* env for the loader CLI, resolved at task runtime (not parse time).

    Infra config comes from Airflow Variables (set in the Astro Environment Manager); the
    bastion fields come from the gp_bastion_host connection and feed the loader's SSH tunnel.
    """
    return {
        "LOADER_ENV": "{{ var.value.get('loader_env', 'dev') }}",
        "LOADER_S3_BUCKET": "{{ var.value.get('loader_s3_bucket', '') }}",
        "LOADER_S3_IMPORT_ROLE_ARN": "{{ var.value.get('loader_s3_import_role_arn', '') }}",
        "LOADER_AWS_ACCOUNT_ID": "{{ var.value.get('loader_aws_account_id', '') }}",
        "LOADER_DATABRICKS_WAREHOUSE_ID": "{{ var.value.get('loader_databricks_warehouse_id', '') }}",
        "LOADER_VPC_ID": "{{ var.value.get('loader_vpc_id', '') }}",
        "LOADER_DB_SUBNET_GROUP": "{{ var.value.get('loader_db_subnet_group', '') }}",
        "LOADER_SECURITY_GROUP_ID": "{{ var.value.get('loader_security_group_id', '') }}",
        "LOADER_KMS_KEY_ARN": "{{ var.value.get('loader_kms_key_arn', '') }}",
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


@dag(
    dag_id="load_people_api_v2",
    schedule="@monthly",
    start_date=pendulum_datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 1, "retry_delay": duration(minutes=5)},
    tags=["people-api", "loader", "DATA-1640"],
)
def load_people_api_v2():
    _step("inspect_prod", "inspect-prod")


load_people_api_v2()
