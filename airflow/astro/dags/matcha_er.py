"""## Matcha entity resolution on a schedule

Runs the Splink entity-resolution container once a week for each of the three
entity types, gates its output, and swaps it into the tables dbt reads.

Each entity is one task group: **match** runs the container as a Kubernetes
pod, **gate** checks what it produced, **swap** renames it into place. matcha
writes a DATED table (`clustered_candidacy_stages_20260825`) and never a live
one — its upload is `CREATE OR REPLACE TABLE` followed by `COPY INTO`, so
aiming it at a live table would let a mid-upload failure leave every downstream
dbt model reading an empty or partial table.

The three entities carry no dependency edges between them WITHIN THIS DAG:
each match task depends only on `dbt_refresh_prematch`, so one entity failing
does not block another from matching, gating, or swapping. That does not mean
the entities are independent downstream of `er_source` — `marts/civics/
candidacy_stage.sql` joins the candidacy clustered table with
`ref("election_stage")`, which derives from `clustered_election_stages`, so a
civics mart can read a mix of one entity's fresh vintage and another's stale
one regardless of what this DAG does. What serialises the pods today is the
one-slot `matcha_er` pool, a quota accommodation rather than a modelling
decision — three 8Gi pods in parallel is 24Gi against a 20Gi deployment
quota. Raising the quota and widening the pool needs no change here.

`dbt_build_er_source` waits on all three swaps, so THIS DAG's own staging
rebuild never runs against a partially-swapped set. That is not the same as
`er_source` itself staying consistent: if one entity's swap fails after the
other two have already replaced their live tables, those two ARE published —
`er_source` already holds a mix of this run's fresh tables and whichever
vintage the failed entity's live table was last swapped from. Any other job
reading `er_source` before a retry succeeds sees that mix; only this DAG's
own `dbt_build_er_source` step is withheld.

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
- `matcha_image_tag` — matcha image tag to run. Defaults to `latest`; set to
  a sha to pin a deployment without a code change.
- `matcha_image_pull_secret` — image pull secret name from Astronomer support.
  Unset means the GHCR package is public and pulls anonymously.

### Pools:
- `matcha_er` — one slot, so only one pod runs at a time.
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
    dated_name,
    drop_old_table,
    drop_stale_vintages,
    open_connection,
    run_gate,
    swap_enabled,
    swap_table,
)
from kubernetes.client import models as k8s
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

# The tag CI publishes beside `latest` on every merge to main, i.e. the only tag form that
# makes a run reproducible. A digest reference (`@sha256:...`) counts as pinned as well.
_PINNED_TAG = re.compile(r"[0-9a-f]{40}")

# `image` is a KPO template field, so the tag resolves at task runtime, not parse — any merge
# touching matcha/** would otherwise silently change what the next scheduled run executes with
# no code change to show for it. A deployment can pin a sha via the Variable with no redeploy;
# the default keeps today's behavior.
MATCHA_IMAGE_TAG_VARIABLE = "matcha_image_tag"
MATCHA_IMAGE = (
    "ghcr.io/thegoodparty/gp-data-platform/matcha:"
    f"{{{{ var.value.get('{MATCHA_IMAGE_TAG_VARIABLE}', 'latest') }}}}"
)
MATCHA_POOL = "matcha_er"
# Explicit because Kubernetes otherwise infers it from the tag (Always for `:latest`,
# IfNotPresent for anything else), so pinning the tag Variable would flip pull behavior as a
# side effect. Always over IfNotPresent: a node-local cache can hold a matcher build older
# than the tag now points at and would run it silently, and it buys little coherence between
# this run's pods, which the pool serializes onto generally separate nodes.
MATCHA_IMAGE_PULL_POLICY = "Always"
# A hung Splink pod would otherwise hold the single pool slot indefinitely, blocking the other
# two entities and the following week's run. startup_timeout_seconds only bounds scheduling.
MATCH_EXECUTION_TIMEOUT = duration(hours=4)
IMAGE_PULL_SECRET_VARIABLE = "matcha_image_pull_secret"
ER_SCHEMA = "er_source"
DBT_SCHEMA = "dbt"
CATALOG_VARIABLE = "databricks_catalog"
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
        secret_name = Variable.get(IMAGE_PULL_SECRET_VARIABLE, default="")
        if secret_name:
            self.image_pull_secrets = [k8s.V1LocalObjectReference(name=secret_name)]
        self._log_image_provenance()
        super().pre_execute(context)

    def _log_image_provenance(self) -> None:
        """Record which image this pod is about to run, and whether it is pinned.

        `image` is a template field, so what a run actually executed is only
        knowable from the run's own logs. A mutable tag additionally means the
        three entity pods of one run are not guaranteed to be the same build:
        the pool serializes them, so a merge touching `matcha/**` landing
        between two pods republishes `latest` and the later pod runs different
        matcher code. Nothing is corrupted by that — each entity's tables come
        from a single pod — but a gate failure stops being attributable to the
        data rather than to a matcher change, which is the one distinction the
        vintage-and-gate design exists to make. Pinning the tag to the sha CI
        publishes on every merge removes the ambiguity; the warning says so at
        the point where the run is about to pay for not having done it.
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
        execution_timeout=MATCH_EXECUTION_TIMEOUT,
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
    # gate/swap/cleanup are unpooled (only the pods are), so an overlapping manual trigger
    # would give two runs with different ds_nodash whose swaps can interleave DROP/RENAME on
    # the same live table.
    max_active_runs=1,
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
                if not swap_enabled():
                    t_log.info(
                        "%s is not 'true' — rehearsal only, leaving %s live tables untouched.",
                        SWAP_GATE_VARIABLE,
                        entity.entity_type,
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
