"""## Gold-match daily loop

Matches the day's pending offices in the gold-match container, rebuilds the
warehouse so the results serve, gates the published labels, and repairs
itself on any failure.

The pod does exactly what the supervised entry point does (read cohort,
match, write under the run key this DAG passes in); the DAG owns everything
around it: the post-write dbt rebuild, the label gates, a cleanup finalizer
(cancel any live rebuild, delete the run's rows, ALWAYS rebuild, re-raise),
and one notification-only `operator_signal` leaf. Every alert-worthy state is
a failed DAG run on the existing failed-DAG Slack alert, and the failing
TASK's name carries the story — see `docs/gold_match_daily.md`.

Schedule contract (in place of any dependency wiring, by design): 14:30 UTC
sits after the day's two universe-moving events (08:00 L2 load, 12:02 build).
The election-api sync runs at 22:00 UTC; a typical failure day's delete +
repair rebuild ends ~19:30, and the one path that can brush the sync (a
repair rebuild running out the finalizer's full ceiling) is exactly the case
the runbook's swap-gate rule exists for.

The DAG deploys `is_paused_upon_creation=True`. BUILD must not schedule
anything; unpausing is the owner-gated activation checklist.

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M, selected
  by the `databricks_conn_id` Variable; shared with the other DAGs.
- `dbt_cloud` — dbt Cloud API, shared with the other DAGs.

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects the Databricks connection.
- `gold_match_image_tag` — REQUIRED: the gate-passed build's sha (the
  evaluated artifact is the production artifact). Unset fails the run at
  render; `latest` works only as an explicit, provenance-warned override.
- `gold_match_image_pull_secret` — image pull secret name from Astronomer
  support (the GHCR package is private; the pull secret is required).
- `BRAINTRUST_API_KEY` — injected into the pod; the pinned prompt fails
  closed without it.
"""

from __future__ import annotations

import logging
import re

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.sdk import Variable, dag, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.gold_match_utils import (
    GOLD_MATCH_REBUILD_JOB_ID,
    cancel_dbt_run_and_confirm,
    delete_run_rows,
    gold_match_pod_env,
    new_quarantine_count,
    run_gate_queries,
    run_key_of,
    trigger_rebuild_and_wait,
)
from kubernetes.client import models as k8s
from pendulum import datetime as pendulum_datetime
from pendulum import duration

t_log = logging.getLogger("airflow.task")

# Same pinned-image contract as matcha: CI publishes a 40-hex sha tag beside
# `latest` on every merge; a digest reference counts as pinned too.
_PINNED_TAG = re.compile(r"[0-9a-f]{40}")

GOLD_MATCH_IMAGE_TAG_VARIABLE = "gold_match_image_tag"
# REQUIRED, no `latest` default, unlike matcha: an unattended loop on a
# mutable tag silently runs whatever main last published, and the evaluated
# artifact must be the production artifact. The Variable holds the
# gate-passed build's sha; an unset Variable fails the task at render
# (loudly, before any pod runs), and `latest` works only as an explicit,
# provenance-warned override.
GOLD_MATCH_IMAGE = (
    "ghcr.io/thegoodparty/gp-data-platform/gold-match:" f"{{{{ var.value.{GOLD_MATCH_IMAGE_TAG_VARIABLE} }}}}"
)
IMAGE_PULL_SECRET_VARIABLE = "gold_match_image_pull_secret"
# Steady state is minutes; a wave day (~8k offices) adds roughly an hour, so
# 3h bounds a hung pod without cutting a legitimate wave short.
MATCH_EXECUTION_TIMEOUT = duration(hours=3)
REBUILD_TIMEOUT_S = 3 * 3600
# The rollback wording is confined to the supervised rollback path: a cause
# string is an interface, and a healthy rebuild labeled "rollback" once got
# cancelled by a teammate acting reasonably on what it said.
POST_WRITE_CAUSE_PREFIX = "gold-match daily: post-write rebuild"
CLEANUP_CAUSE_PREFIX = "gold-match daily: cleanup rebuild after failed run"


class _GoldMatchPodOperator(KubernetesPodOperator):
    """KPO that resolves its pull secret and pod env at task runtime.

    matcha's pattern, for matcha's reasons: Astro exposes no Variables to the
    DAG processor at parse, and Airflow snapshots rendered template fields
    (env_vars included, plus the whole pod YAML) into the metadata DB BEFORE
    pre_execute runs — credentials resolved here never reach that snapshot.
    """

    def pre_execute(self, context) -> None:
        secret_name = Variable.get(IMAGE_PULL_SECRET_VARIABLE, default="")
        if secret_name:
            self.image_pull_secrets = [k8s.V1LocalObjectReference(name=secret_name)]
        # Replaces rather than extends: pre_execute runs again on every retry.
        # The image-baked GIT_SHA env is untouched by pod-level env vars.
        self.env_vars = [k8s.V1EnvVar(name=name, value=value) for name, value in gold_match_pod_env().items()]
        self._log_image_provenance()
        super().pre_execute(context)

    def _log_image_provenance(self) -> None:
        """A mutable tag means a gate failure cannot be attributed to the data
        over a matcher change; the run's own logs must say which build ran."""
        image = self.image or ""
        _, _, tag = image.rpartition(":")
        if "@sha256:" in image or _PINNED_TAG.fullmatch(tag):
            t_log.info("gold-match image pinned for this run: %s", image)
            return
        t_log.warning(
            "gold-match image %s is a mutable tag; pin the %s Variable to the sha tag CI "
            "publishes beside `latest` for a reproducible run.",
            image,
            GOLD_MATCH_IMAGE_TAG_VARIABLE,
        )


def _match_pod() -> _GoldMatchPodOperator:
    """The container run: match the pending cohort, write under the run key."""
    return _GoldMatchPodOperator(
        task_id="match_pod",
        name="gold-match-daily",
        image=GOLD_MATCH_IMAGE,
        image_pull_policy="Always",
        arguments=[
            "stitch_golden_data.prod_gold_data.daily_run",
            "--run-key",
            # The DagRun's own aware timestamp: stable for the run's lifetime,
            # so cleanup can always address the rows even when the pod died
            # mid-write. The entry point truncates it to whole seconds.
            "{{ dag_run.start_date }}",
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "8Gi", "cpu": "4"},
            limits={"memory": "8Gi", "cpu": "4"},
        ),
        in_cluster=True,
        get_logs=True,
        on_finish_action="delete_pod",
        # The daily retry is tomorrow's run: a same-key retry would reopen the
        # resume/overlap/count-reconciliation states the design removed.
        retries=0,
        execution_timeout=MATCH_EXECUTION_TIMEOUT,
    )


@dag(
    dag_id="gold_match_daily",
    schedule="30 14 * * *",
    start_date=pendulum_datetime(2026, 9, 2, tz="UTC"),
    catchup=False,
    # BUILD deploys paused; unpausing is the owner-gated activation.
    is_paused_upon_creation=True,
    # A manual trigger during a scheduled run queues instead of creating a
    # second writer against the same tables.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": duration(minutes=10)},
    tags=["gold-match"],
)
def gold_match_daily():
    match_pod = _match_pod()

    rebuild = DbtCloudRunJobOperator(
        task_id="rebuild",
        dbt_cloud_conn_id="dbt_cloud",
        job_id=GOLD_MATCH_REBUILD_JOB_ID,
        trigger_reason=POST_WRITE_CAUSE_PREFIX + " (run {{ dag_run.start_date }})",
        wait_for_termination=True,
        timeout=REBUILD_TIMEOUT_S,
        check_interval=60,
        # One ~2h attempt, deliberately: the pre-sync margin (the election-api
        # sync runs at 22:00 UTC) assumes no retry loops here, and a
        # deterministic failure — the coverage floor runs in this job at error
        # severity — would fail identically anyway. Cleanup restores
        # yesterday's state; tomorrow's run is the retry.
        retries=0,
    )

    @task(execution_timeout=duration(minutes=30))
    def gates(dag_run=None) -> int:
        """Fails ONLY when destroying this run is the remedy; every other
        needs-a-human observation rides operator_signal, which cleanup never
        listens to."""
        run_key = run_key_of(dag_run)
        conn = connect_from_conn_id()
        try:
            metrics = run_gate_queries(conn, run_key)
        finally:
            conn.close()
        if metrics["run_scoped_dead"] > 0:
            raise AirflowException(
                f"{metrics['run_scoped_dead']} label tuple(s) matched by THIS run are dead in "
                "the current universe — destroying this run is the remedy; cleanup follows"
            )
        # The global count travels to operator_signal by XCom: an older run's
        # dead tuple needs a human, not a deletion.
        return metrics["global_dead"]

    @task(trigger_rule="all_done", execution_timeout=duration(minutes=15))
    def operator_signal(dag_run=None, ti=None) -> None:
        """Notification-only leaf: fails (so the DAG fails and the alert
        fires) on what a human must see that is NOT this run's fault. Runs on
        all_done so a first quarantine still signals when the rebuild or gates
        failed afterward; it reads the quarantine table, which survives
        cleanup (cleanup deletes result rows only)."""
        run_key = run_key_of(dag_run)
        conn = connect_from_conn_id()
        try:
            fresh = new_quarantine_count(conn, run_key)
        finally:
            conn.close()
        problems = []
        if fresh:
            problems.append(f"{fresh} office(s) first entered quarantine this run")
        global_dead = ti.xcom_pull(task_ids="gates")
        if global_dead:
            problems.append(
                f"{global_dead} dead label tuple(s) in the serving state from OLDER runs; "
                "deleting this run cannot clear them — repair at source"
            )
        if problems:
            raise AirflowException("needs a human, nothing deleted: " + "; ".join(problems))
        t_log.info("nothing to signal")

    # 4h, strictly above the repair rebuild's own 3h wait: an outer bound at or
    # below the inner one would kill cleanup mid-repair.
    @task(trigger_rule="one_failed", retries=0, execution_timeout=duration(hours=4))
    def cleanup_finalizer(dag_run=None, ti=None) -> None:
        """Cancel any live rebuild and CONFIRM it terminal, delete this run's
        rows by key, ALWAYS trigger a repair rebuild and wait, then re-raise
        so the DAG run ends FAILED. retries=0: the default retry policy must
        not loop the ~100-minute repair cycle toward the 22:00 sync."""
        run_key = run_key_of(dag_run)
        hook = DbtCloudHook("dbt_cloud")
        # Pushed by the operator BEFORE it waits, so it survives a timeout or
        # a mid-poll crash; absent means the trigger itself never fired and
        # there is nothing to cancel. An UNCONFIRMED cancel deliberately stops
        # everything here (no delete, no repair): that is the runbook's
        # swap-gate story, not a state this task can safely write over.
        rebuild_run_id = ti.xcom_pull(task_ids="rebuild", key="job_run_id")
        if rebuild_run_id:
            cancel_dbt_run_and_confirm(hook, rebuild_run_id)
        # The supervised rollback's shape (backlog_run): a DELETE can commit
        # and then raise on its response or teardown, so the failure is
        # captured and the repair rebuild runs REGARDLESS -- otherwise serving
        # keeps ghost rows the source no longer has.
        deleted = None
        delete_error: Exception | None = None
        try:
            conn = connect_from_conn_id()
            try:
                deleted = delete_run_rows(conn, run_key)
            finally:
                conn.close()
        except Exception as exc:
            delete_error = exc
        trigger_rebuild_and_wait(hook, cause=f"{CLEANUP_CAUSE_PREFIX} (run {run_key.isoformat()})")
        if delete_error is not None:
            raise AirflowException(
                f"cleanup delete raised for run {run_key.isoformat()} (the repair rebuild ran; "
                "the run's rows may still exist -- delete by key and rebuild again)"
            ) from delete_error
        raise AirflowException(
            f"cleanup completed after an upstream failure (deleted {deleted} result row(s); "
            "repair rebuild succeeded) — see the failed upstream task for the cause"
        )

    gates_task = gates()
    signal_task = operator_signal()
    cleanup_task = cleanup_finalizer()

    match_pod >> rebuild >> gates_task
    [match_pod, gates_task] >> signal_task
    # Trigger-set membership IS the contract: one_failed fires on a failed OR
    # upstream_failed direct upstream (verified in the installed scheduler's
    # trigger_rule_dep), so what keeps a deliberate operator_signal failure
    # from ever destroying a gated run is that it is NOT in this list.
    [match_pod, rebuild, gates_task] >> cleanup_task


gold_match_daily()
