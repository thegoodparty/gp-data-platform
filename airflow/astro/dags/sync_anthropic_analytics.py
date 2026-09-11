"""Daily sync of Claude Enterprise Analytics data into Delta tables for the Sigma spend dashboard.

Thin sequencer over the `anthropic-analytics-loader` CLI (installed on the worker image via
astro/requirements.txt, same git-subdirectory pattern as `loader` for load_people_api). One
BashOperator, run with no date args so it defaults to a 3-day lookback -- idempotent (delete +
reinsert per table's date range), so the overlap with the previous run's window is harmless and
absorbs Anthropic's up-to-~24h data revision window.

Databricks credentials are derived the same way load_people_api's `unload` step gets them: OAuth
M2M from the shared `databricks_conn_id` Airflow Variable's connection, resolved at task runtime
(not DAG parse). `ANTHROPIC_ANALYTICS_API_KEY` is NOT templated here -- it's an Astro Environment
Variable (Secret), which `append_env=True` forwards into the task's subprocess ambiently.
"""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from pendulum import datetime as pendulum_datetime
from pendulum import duration

_DBX_CONN_EXPR = "conn.get(var.value.get('databricks_conn_id', 'databricks'))"
_DBX_ENV: dict[str, str] = {
    "DATABRICKS_HOST": "{% set c = " + _DBX_CONN_EXPR + " %}{{ c.host }}",
    "DATABRICKS_CLIENT_ID": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.login or c.extra_dejson.get('client_id', '') }}",
    "DATABRICKS_CLIENT_SECRET": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.password or c.extra_dejson.get('client_secret', '') }}",
    "LOADER_DATABRICKS_WAREHOUSE_ID": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.extra_dejson.get('http_path', '').rstrip('/').split('/') | last }}",
}


@dag(
    dag_id="sync_anthropic_analytics",
    schedule="@daily",
    start_date=pendulum_datetime(2026, 9, 11, tz="UTC"),
    catchup=False,
    # Created paused, like the other prod DAGs -- a fresh deploy shouldn't auto-fire.
    is_paused_upon_creation=True,
    default_args={"retries": 3, "retry_delay": duration(minutes=5)},
    tags=["anthropic-analytics", "loader", "sigma"],
)
def sync_anthropic_analytics():
    BashOperator(
        task_id="sync",
        bash_command="anthropic-analytics-loader",
        env=_DBX_ENV,
        append_env=True,
    )


sync_anthropic_analytics()
