"""People-API serving cutover (manual).

Promotes the cluster a completed `load_people_api` run built into serving: the `promote` loader
step copies that run's connection string into the single serving SSM parameter and labels the
new version `refresh-{date}`. Kept in its own DAG with `schedule=None` so it NEVER fires
automatically — an operator triggers it after reviewing that run's validate + analyze, passing
`{"run_date": "YYYYMMDD"}` in the trigger config to pick which run to promote (defaulting to the
triggered run's logical date).

people-api's DatabaseUrlProvider re-reads the serving parameter every ~5 min and hot-swaps, so
cutover needs no service restart. ROLLBACK: the prior value stays in the serving parameter's
version history under its `refresh-{prev_date}` label; re-put it to swap back. That requires the
PRIOR cluster to still exist, so do NOT teardown the previous cluster until this one is confirmed
healthy.

promote touches only S3 (manifests) and SSM, so — unlike the load steps — it needs no bastion
tunnel or Databricks creds; the loader's LOADER_*/ENVIRONMENT config reaches it via append_env.
"""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from pendulum import datetime as pendulum_datetime
from pendulum import duration


@dag(
    dag_id="promote_people_api",
    schedule=None,  # manual trigger only — this is the gated prod serving cutover
    start_date=pendulum_datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    # promote is idempotent (a completed manifest short-circuits; the SSM put/label are safe to
    # repeat), so auto-retrying a transient SSM/network blip is fine and keeps the repo's
    # retries>=2 convention.
    default_args={"retries": 3, "retry_delay": duration(minutes=5)},
    tags=["people-api", "loader", "cutover"],
)
def promote_people_api():
    # run_date selects which completed load_people_api run to promote; pass it in the trigger
    # config ({"run_date": "YYYYMMDD"}), falling back to this run's logical date.
    BashOperator(
        task_id="promote",
        bash_command="loader promote --date {{ dag_run.conf.get('run_date', ds_nodash) }}",
        append_env=True,
    )


promote_people_api()
