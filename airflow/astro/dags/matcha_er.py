"""## Matcha entity resolution on a schedule

Runs the Splink entity-resolution container weekly for each of the three entity
types, gates its output, and swaps it into the tables dbt reads. One task group
per entity: **match** runs the container as a Kubernetes pod, **gate** checks
what it produced, **swap** renames it into place.

matcha writes a DATED table and never a live one, because its upload is
`CREATE OR REPLACE TABLE` then `COPY INTO` -- aimed at a live table, a
mid-upload failure would leave downstream dbt reading a partial one.

The three entities have no dependency edges between them here: each match
depends only on `dbt_refresh_prematch`. `max_active_tasks=1` serialises them
anyway, which is a quota accommodation, not a modelling decision.

`docs/matcha_er.md` covers the Variables and Connections this expects, why dev
needs its own schema, rehearsal vs. live, and the gate/swap recovery paths.
"""

from __future__ import annotations

import logging
import re

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.sdk import Variable, dag, task, task_group
from include.custom_functions.matcha_utils import (
    ENTITIES,
    SWAP_GATE_VARIABLE,
    EntitySpec,
    create_schema_if_missing,
    dated_name,
    drop_old_table,
    drop_stale_vintages,
    open_connection,
    pod_databricks_env,
    run_gate,
    swap_enabled,
    swap_table,
)
from kubernetes.client import models as k8s
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

# The sha tag CI publishes beside `latest`; a `@sha256:` digest counts as pinned too.
_PINNED_TAG = re.compile(r"[0-9a-f]{40}")

# A Variable so a deployment can pin a sha with no redeploy. `image` is a KPO template
# field, so it resolves at task runtime.
MATCHA_IMAGE_TAG_VARIABLE = "matcha_image_tag"
MATCHA_IMAGE = (
    "ghcr.io/thegoodparty/gp-data-platform/matcha:"
    f"{{{{ var.value.get('{MATCHA_IMAGE_TAG_VARIABLE}', 'latest') }}}}"
)
# Explicit: Kubernetes otherwise infers it from the tag, so pinning the tag would flip pull
# behaviour as a side effect. IfNotPresent would let a node run a stale cached build.
MATCHA_IMAGE_PULL_POLICY = "Always"
# A hung pod would otherwise hold the single task slot indefinitely.
# startup_timeout_seconds only bounds scheduling, not the run.
MATCH_EXECUTION_TIMEOUT = duration(hours=4)
# One catalog serves both environments, so the schema is what separates them: hardcoded, a
# dev run would fight prod for the same dated table names and its swap would rename the live
# tables the civics marts read.
ER_SCHEMA_VARIABLE = "databricks_er_schema"
DEFAULT_ER_SCHEMA = "er_source"
# Templated for the pod's arguments; Astro exposes no Variables at parse time.
ER_SCHEMA_TEMPLATE = f"{{{{ var.value.get('{ER_SCHEMA_VARIABLE}', '{DEFAULT_ER_SCHEMA}') }}}}"
DBT_SCHEMA = "dbt"
CATALOG_VARIABLE = "databricks_catalog"
# Weekly schedule, so this keeps roughly a month of vintages to audit against.
VINTAGE_RETENTION_DAYS = 28


def er_schema() -> str:
    """Schema the dated vintages and live tables live in, at task runtime."""
    return Variable.get(ER_SCHEMA_VARIABLE, default=DEFAULT_ER_SCHEMA)


class _MatchaPodOperator(KubernetesPodOperator):
    """KPO that resolves its Databricks env at task runtime.

    Runtime rather than templated because Airflow snapshots rendered template
    fields into the metadata DB and serves them in the UI. Resolving after that
    snapshot leaves nothing to redact: the credential only ever reaches the pod
    spec this operator submits. The GHCR package is public, so no pull secret.
    """

    def pre_execute(self, context) -> None:
        # Replaces rather than extends: pre_execute runs again on every retry.
        self.env_vars = [k8s.V1EnvVar(name=name, value=value) for name, value in pod_databricks_env().items()]
        # KPO prints the pod spec only on failure, so a passing run would otherwise
        # record nothing about its sizing.
        resources = self.container_resources
        self.log.info("match pod resources: %s", resources.limits if resources else None)
        self._log_image_provenance()
        super().pre_execute(context)

    def _log_image_provenance(self) -> None:
        """Record which image this pod runs, and whether it is pinned.

        On a mutable tag the run's pods are not guaranteed to be the same build,
        since a merge landing mid-run republishes `latest`. Nothing corrupts --
        each entity's tables come from one pod -- but a gate failure stops being
        attributable to the data rather than to a matcher change.
        """
        image = self.image or ""
        _, _, tag = image.rpartition(":")
        if "@sha256:" in image or _PINNED_TAG.fullmatch(tag):
            t_log.info("matcha image pinned for this run: %s", image)
            return
        t_log.warning(
            "matcha image %s is a mutable tag: the pods in this run are not guaranteed to be "
            "the same build, so a gate failure here cannot be attributed to the data over a "
            "matcher change. Pin the %s Variable to the sha tag CI publishes beside `latest` "
            "for a reproducible run.",
            image,
            MATCHA_IMAGE_TAG_VARIABLE,
        )


POD_MEMORY = "48Gi"
POD_CPU = "4"
POD_EPHEMERAL_STORAGE = "50Gi"


def _pod_resources() -> k8s.V1ResourceRequirements:
    """Match pod resources, requests equal to limits.

    Equal keeps the pod Guaranteed, so it is evicted last. Astro does this to
    task pods anyway, and bills on the limit.
    """
    quantities = {
        "memory": POD_MEMORY,
        "cpu": POD_CPU,
        "ephemeral-storage": POD_EPHEMERAL_STORAGE,
    }
    return k8s.V1ResourceRequirements(requests=dict(quantities), limits=dict(quantities))


def _match_pod(entity: EntitySpec) -> _MatchaPodOperator:
    """The container run for one entity, writing this run's dated vintage."""
    catalog = "{{ var.value.get('databricks_catalog') }}"
    dated_cluster = dated_name(entity.cluster_table, "{{ ds_nodash }}")
    dated_pairwise = dated_name(entity.pairwise_table, "{{ ds_nodash }}")
    return _MatchaPodOperator(
        task_id="match",
        name=f"matcha-{entity.entity_type.replace('_', '-')}",
        image=MATCHA_IMAGE,
        image_pull_policy=MATCHA_IMAGE_PULL_POLICY,
        arguments=[
            "match",
            "--entity-type",
            entity.entity_type,
            "--input",
            f"{catalog}.{DBT_SCHEMA}.{entity.prematch_model}",
            "--output-cluster-table",
            f"{catalog}.{ER_SCHEMA_TEMPLATE}.{dated_cluster}",
            "--output-pairwise-table",
            f"{catalog}.{ER_SCHEMA_TEMPLATE}.{dated_pairwise}",
            "--overwrite",
            # The gate checks the real tables, and matcha's audit CSVs are written
            # into the pod filesystem and die with it.
            "--no-audit",
        ],
        # DuckDB takes 80% of the memory limit and one thread per CPU, so 48Gi gives it
        # ~38 GiB and leaves ~10Gi for the Python side -- which is what 32Gi ran out of,
        # OOM-killing election_stage while writing ~20M pairs. 16 CPU made no difference.
        # ephemeral-storage is declared because Astro's namespace default is 256Mi.
        container_resources=_pod_resources(),
        # A match fails deterministically and each attempt burns a four-hour pod.
        retries=0,
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        execution_timeout=MATCH_EXECUTION_TIMEOUT,
    )


@dag(
    dag_id="matcha_er",
    schedule="@weekly",
    start_date=pendulum_datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    # catchup=False only suppresses historical backfill, not the current interval.
    is_paused_upon_creation=True,
    default_args={"retries": 2, "retry_delay": duration(minutes=10)},
    tags=["matcha", "er"],
    # One 48Gi match pod at a time, against a 96Gi deployment quota. Deliberately not an
    # Airflow pool: a pool must exist on each deployment out of band, and a missing one parks
    # tasks in `scheduled` forever. The cost is a finished entity's gate/swap waiting behind
    # the next entity's match -- seconds of SQL behind hours of Splink.
    max_active_tasks=1,
    # Two runs with different ds_nodash can interleave DROP/RENAME on one live table.
    max_active_runs=1,
)
def matcha_er():
    # `dbt build` is run plus test, so bad input fails here rather than inside Splink.
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

    @task(task_id="ensure_er_schema")
    def ensure_er_schema() -> str:
        """Create this deployment's ER schema if absent, before any pod writes.

        Whoever creates it owns it, so a deployment pointed at its own schema
        needs no grant beyond the catalog-level CREATE_SCHEMA the airflow SPs
        hold. Pointed at someone else's schema this is a no-op.
        """
        catalog = Variable.get(CATALOG_VARIABLE)
        schema = er_schema()
        conn = open_connection()
        try:
            create_schema_if_missing(conn, catalog, schema)
        finally:
            conn.close()
        return f"{catalog}.{schema}"

    @task(task_id="cleanup")
    def cleanup(run_date: str) -> dict[str, list[str]]:
        """Drop the renamed-aside tables and vintages past the retention window.

        A rehearsal run keeps `_old`: no swap promoted anything, so it belongs to
        whichever run last swapped for real and is that vintage's only rollback
        path. Safe to keep, since the next live swap pre-drops it. Stale vintages
        are reaped either way -- a consumed vintage is never a rollback position.
        """
        catalog = Variable.get(CATALOG_VARIABLE)
        schema = er_schema()
        # One read per task, so a mid-task flip cannot drop some entities' backups only.
        drop_backups = swap_enabled()
        if not drop_backups:
            t_log.info(
                "%s is not 'true' — rehearsal, so keeping every _old table as the rollback "
                "position from the last live swap.",
                SWAP_GATE_VARIABLE,
            )
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
                    if drop_backups:
                        drop_old_table(conn, catalog, schema, table)
                    dropped[table] = drop_stale_vintages(conn, catalog, schema, table, cutoff)
        finally:
            conn.close()
        return dropped

    def entity_group(entity: EntitySpec):
        """Build one entity's match -> gate -> swap chain.

        A factory, not a loop body: closing over the loop variable would
        late-bind every group to the last entity.
        """

        @task_group(group_id=entity.entity_type)
        def group():
            match = _match_pod(entity)

            @task(task_id="gate")
            def gate(run_date: str) -> None:
                catalog = Variable.get(CATALOG_VARIABLE)
                schema = er_schema()
                conn = open_connection()
                try:
                    for table, table_gate in (
                        (entity.cluster_table, entity.cluster_gate),
                        (entity.pairwise_table, entity.pairwise_gate),
                    ):
                        run_gate(
                            conn,
                            catalog,
                            schema,
                            table,
                            dated_name(table, run_date),
                            table_gate,
                        )
                finally:
                    conn.close()

            @task(task_id="swap")
            def swap(run_date: str) -> None:
                if not swap_enabled():
                    t_log.info(
                        "%s is not 'true' — rehearsal only, leaving %s live tables untouched.",
                        SWAP_GATE_VARIABLE,
                        entity.entity_type,
                    )
                    return
                catalog = Variable.get(CATALOG_VARIABLE)
                schema = er_schema()
                conn = open_connection()
                try:
                    for table in (entity.cluster_table, entity.pairwise_table):
                        swap_table(conn, catalog, schema, table, dated_name(table, run_date))
                finally:
                    conn.close()

            match >> gate("{{ ds_nodash }}") >> swap("{{ ds_nodash }}")

        return group()

    groups = [entity_group(entity) for entity in ENTITIES]
    refresh_prematch >> ensure_er_schema() >> groups >> build_downstream >> cleanup("{{ ds_nodash }}")


matcha_er()
