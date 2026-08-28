"""Retire a people-api voter cluster, keeping a restore point.

Standalone, manually-triggered counterpart to the `teardown` step in load_people_api: it
deletes only the COMPUTE (writer instance + cluster) of a prior dated cluster
(`gp-people-db-{date}-{env}`) while keeping the artifacts you'd need to bring it back —
a final cluster snapshot, the dated connection-string SSM parameter, and the DB parameter
groups. Use it to reclaim Aurora cost from superseded clusters (e.g. an old prod-{date})
without losing the ability to restore.

Safety: the loader's teardown is name-scoped to `gp-people-db-{date}*`, so it can never touch
the live serving cluster (`gp-people-db-{env}`) or shared infra; and this DAG defaults to a
DRY RUN — set `dry_run=false` to actually delete. AWS credentials come from the worker's
standard credential chain; the loader's config (ENVIRONMENT, LOADER_*) reaches the subprocess
via append_env. No bastion/Databricks needed — teardown never connects to the database.
"""

from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from pendulum import datetime as pendulum_datetime
from pendulum import duration


@dag(
    dag_id="teardown_people_api_cluster",
    schedule=None,  # manual trigger only
    start_date=pendulum_datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": duration(minutes=5)},
    tags=["people-api", "loader", "teardown"],
    params={
        "date": Param(
            "",
            type="string",
            title="Cluster date (ds_nodash)",
            description="Date of the cluster to retire, e.g. 20260728 for gp-people-db-20260728-{env}.",
        ),
        "dry_run": Param(
            True,
            type="boolean",
            title="Dry run",
            description="Preview only — lists what would be deleted. Uncheck to actually delete.",
        ),
    },
)
def teardown_people_api_cluster():
    BashOperator(
        task_id="retire_cluster",
        # Keep the restore set (snapshot + dated SSM param + param groups); delete only the compute.
        # dry_run (default) omits --confirm, so the loader lists its plan without touching anything.
        bash_command=(
            "loader teardown --date {{ params.date }} "
            "--snapshot --keep-ssm --keep-param-groups"
            "{% if not params.dry_run %} --confirm{% endif %}"
        ),
        append_env=True,
    )


teardown_people_api_cluster()
