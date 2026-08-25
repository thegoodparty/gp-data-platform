"""## Matcha entity resolution on a schedule

Runs the Splink entity-resolution container once a week for each of the three
entity types, gates its output, and swaps it into the tables dbt reads.

Each entity is one task group: **match** runs the container as a Kubernetes
pod, **gate** checks what it produced, **swap** renames it into place. matcha
writes a DATED table (`clustered_candidacy_stages_20260825`) and never a live
one — its upload is `CREATE OR REPLACE TABLE` followed by `COPY INTO`, so
aiming it at a live table would let a mid-upload failure leave every downstream
dbt model reading an empty or partial table.

The three entities carry no dependency edges between them: no dbt model joins
two entities' cluster tables, so one failing must not block the others. What
serialises the pods today is the one-slot `matcha_er` pool, a quota
accommodation rather than a modelling decision — three 8Gi pods in parallel is
24Gi against a 20Gi deployment quota. Raising the quota and widening the pool
needs no change here.

`dbt_build_er_source` waits on all three swaps, so a partial failure withholds
the downstream build rather than publishing a mixed vintage.

The swap is held behind the `matcha_swap_enabled` Variable: anything but
"true" withholds only the rename, making every run a full dress rehearsal that
still builds and gates the dated tables.

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M.
- `dbt_cloud` — dbt Cloud API, shared with the other DAGs.

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects the Databricks connection.
- `databricks_catalog` — Databricks catalog name.
- `dbt_cloud_job_id` — dbt Cloud job the bookends run steps against.
- `matcha_swap_enabled` — cutover switch. Anything but "true" is rehearsal.
- `matcha_image_pull_secret` — image pull secret name from Astronomer support.
  Unset means the GHCR package is public and pulls anonymously.

### Pools:
- `matcha_er` — one slot, so only one pod runs at a time.
"""

from __future__ import annotations

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.sdk import Variable, dag, task, task_group
from include.custom_functions.matcha_utils import (
    ENTITIES,
    EntitySpec,
    dated_name,
    drop_old_table,
    drop_stale_vintages,
    open_connection,
    run_gate,
    swap_table,
)
from kubernetes.client import models as k8s
from pendulum import datetime as pendulum_datetime
from pendulum import duration

MATCHA_IMAGE = "ghcr.io/thegoodparty/gp-data-platform/matcha:latest"
MATCHA_POOL = "matcha_er"
ER_SCHEMA = "er_source"
DBT_SCHEMA = "dbt"
CATALOG_VARIABLE = "databricks_catalog"
SWAP_GATE_VARIABLE = "matcha_swap_enabled"
IMAGE_PULL_SECRET_VARIABLE = "matcha_image_pull_secret"
# Weekly schedule, so this keeps roughly a month of vintages to audit against.
VINTAGE_RETENTION_DAYS = 28

# Databricks creds for the pod, reused from the SAME connection the other DAGs use.
# WHICH connection is chosen at task RUNTIME from the shared `databricks_conn_id`
# Variable, NOT from a parse-time env var — Astro does not expose deployment env vars
# to the DAG processor at parse. login/password is the standard SP-OAuth storage, with
# an extra_dejson fallback.
_DBX_CONN_EXPR = "conn.get(var.value.get('databricks_conn_id', 'databricks'))"
_DBX_ENV: dict[str, str] = {
    "DATABRICKS_HOST": "{% set c = " + _DBX_CONN_EXPR + " %}{{ c.host }}",
    "DATABRICKS_HTTP_PATH": "{% set c = " + _DBX_CONN_EXPR + " %}{{ c.extra_dejson.get('http_path', '') }}",
    "DATABRICKS_CLIENT_ID": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.login or c.extra_dejson.get('client_id', '') }}",
    "DATABRICKS_CLIENT_SECRET": "{% set c = "
    + _DBX_CONN_EXPR
    + " %}{{ c.password or c.extra_dejson.get('client_secret', '') }}",
}


class _MatchaPodOperator(KubernetesPodOperator):
    """KPO that resolves its image pull secret at task runtime.

    Astro exposes neither Variables nor deployment env vars to the DAG
    processor at parse, and `image_pull_secrets` holds Kubernetes client
    objects rather than strings, so Jinja cannot reach it. Resolving in
    pre_execute keeps parse metastore-free while letting each deployment carry
    its own secret: astro-dev receives one from Astronomer support before
    astro-prod does, and until a deployment has one the matcha GHCR package is
    public and the kubelet pulls it anonymously.
    """

    def pre_execute(self, context) -> None:
        secret_name = Variable.get(IMAGE_PULL_SECRET_VARIABLE, default_var="")
        if secret_name:
            self.image_pull_secrets = [k8s.V1LocalObjectReference(name=secret_name)]
        super().pre_execute(context)


def _match_pod(entity: EntitySpec) -> _MatchaPodOperator:
    """The container run for one entity, writing this run's dated vintage."""
    catalog = "{{ var.value.get('databricks_catalog') }}"
    dated_cluster = dated_name(entity.cluster_table, "{{ ds_nodash }}")
    dated_pairwise = dated_name(entity.pairwise_table, "{{ ds_nodash }}")
    return _MatchaPodOperator(
        task_id="match",
        name=f"matcha-{entity.entity_type.replace('_', '-')}",
        image=MATCHA_IMAGE,
        pool=MATCHA_POOL,
        arguments=[
            "match",
            "--entity-type",
            entity.entity_type,
            "--input",
            f"{catalog}.{DBT_SCHEMA}.{entity.prematch_model}",
            "--output-cluster-table",
            f"{catalog}.{ER_SCHEMA}.{dated_cluster}",
            "--output-pairwise-table",
            f"{catalog}.{ER_SCHEMA}.{dated_pairwise}",
            "--overwrite",
            # The gate checks the real tables, and matcha's audit CSVs are written
            # into the pod filesystem and die with it.
            "--no-audit",
        ],
        env_vars=_DBX_ENV,
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "8Gi", "cpu": "4"},
            limits={"memory": "8Gi", "cpu": "4"},
        ),
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
    )


@dag(
    dag_id="matcha_er",
    schedule="@weekly",
    start_date=pendulum_datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    # Created paused (like the other prod DAGs) so a fresh deploy doesn't auto-fire the
    # current weekly interval — catchup=False only suppresses historical backfill.
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": duration(minutes=10)},
    tags=["matcha", "er"],
)
def matcha_er():
    # `dbt build` is run plus test, so the prematch not-null/unique tests gate the
    # match: bad input fails here rather than inside Splink.
    refresh_prematch = DbtCloudRunJobOperator(
        task_id="dbt_refresh_prematch",
        dbt_cloud_conn_id="dbt_cloud",
        job_id="{{ var.value.dbt_cloud_job_id }}",
        steps_override=["dbt build --select " + " ".join(e.prematch_model for e in ENTITIES)],
        check_interval=30,
        timeout=3600,
    )

    build_downstream = DbtCloudRunJobOperator(
        task_id="dbt_build_er_source",
        dbt_cloud_conn_id="dbt_cloud",
        job_id="{{ var.value.dbt_cloud_job_id }}",
        steps_override=["dbt build --select path:models/staging/er_source+"],
        check_interval=30,
        timeout=3600,
    )

    @task(task_id="cleanup")
    def cleanup(run_date: str) -> dict[str, list[str]]:
        """Drop the renamed-aside tables and vintages past the retention window.

        Runs only after the downstream build succeeds, so `_old` stays available
        as the rollback position for as long as it is useful.
        """
        catalog = Variable.get(CATALOG_VARIABLE)
        cutoff = (
            pendulum_datetime(int(run_date[:4]), int(run_date[4:6]), int(run_date[6:8]))
            .subtract(days=VINTAGE_RETENTION_DAYS)
            .format("YYYYMMDD")
        )
        conn = open_connection()
        dropped: dict[str, list[str]] = {}
        try:
            for entity in ENTITIES:
                for table in (entity.cluster_table, entity.pairwise_table):
                    drop_old_table(conn, catalog, ER_SCHEMA, table)
                    dropped[table] = drop_stale_vintages(conn, catalog, ER_SCHEMA, table, cutoff)
        finally:
            conn.close()
        return dropped

    def entity_group(entity: EntitySpec):
        """Build one entity's match -> gate -> swap chain.

        A factory rather than a loop body: closing over the loop variable
        directly would late-bind every group to the last entity.
        """

        @task_group(group_id=entity.entity_type)
        def group():
            match = _match_pod(entity)

            @task(task_id="gate")
            def gate(run_date: str) -> None:
                catalog = Variable.get(CATALOG_VARIABLE)
                conn = open_connection()
                try:
                    for table, table_gate in (
                        (entity.cluster_table, entity.cluster_gate),
                        (entity.pairwise_table, entity.pairwise_gate),
                    ):
                        run_gate(
                            conn,
                            catalog,
                            ER_SCHEMA,
                            table,
                            dated_name(table, run_date),
                            table_gate,
                        )
                finally:
                    conn.close()

            @task(task_id="swap")
            def swap(run_date: str) -> None:
                if Variable.get(SWAP_GATE_VARIABLE, default_var="") != "true":
                    print(
                        f"{SWAP_GATE_VARIABLE} is not 'true' — rehearsal only, "
                        f"leaving {entity.entity_type} live tables untouched."
                    )
                    return
                catalog = Variable.get(CATALOG_VARIABLE)
                conn = open_connection()
                try:
                    for table in (entity.cluster_table, entity.pairwise_table):
                        swap_table(conn, catalog, ER_SCHEMA, table, dated_name(table, run_date))
                finally:
                    conn.close()

            match >> gate("{{ ds_nodash }}") >> swap("{{ ds_nodash }}")

        return group()

    groups = [entity_group(entity) for entity in ENTITIES]
    refresh_prematch >> groups >> build_downstream >> cleanup("{{ ds_nodash }}")


matcha_er()
