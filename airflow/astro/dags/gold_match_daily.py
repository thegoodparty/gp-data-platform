"""## Gold-match daily loop

Matches the day's pending offices in the gold-match container and lets the
scheduled nightly dbt build publish them.

The pod does exactly what the supervised entry point does (read cohort,
match, write under the run key this DAG passes in). The DAG owns only what
sits around it: an admission check that declines the day, writing nothing,
unless the latest scheduled prod build succeeded and no prod build is in
flight; and one notification-only `operator_signal` leaf. There is no
post-write rebuild, no gate, and no automated delete: the rows the pod writes
reach the internal marts at the next scheduled build (00:02 UTC) and the
product at the following election-api sync (22:00 UTC), and a wrong row found
in the daily audit is deleted by key and its office quarantined by the
operator before that build. Every alert-worthy state is a failed DAG run on
the existing failed-DAG Slack alert, and the failing TASK's name carries the
story — see `docs/gold_match_daily.md`.

Schedule contract (in place of any dependency wiring, by design): 14:30 UTC
sits after the day's two universe-moving events (08:00 L2 load, 12:02 build);
the nightly's tests page on bad rows but do not roll them back (dbt builds a
model before testing it), so the operator's same-day audit with delete-by-key
is the internal-mart stop and the sync's own gates are the product stop; a red
nightly does make admission fail closed the next day, pausing the loop until
the operator removes the offending rows.

The DAG deploys `is_paused_upon_creation=True`. BUILD must not schedule
anything; unpausing is the owner-gated activation checklist.

### Connections (set in Astro Environment Manager):
- `databricks` / `databricks_dev` (Generic) — Databricks OAuth M2M, selected
  by the `databricks_conn_id` Variable; shared with the other DAGs.
- `dbt_cloud` — dbt Cloud API, read-only here (admission asks about runs).

### Variables (set in Astro Environment Manager):
- `databricks_conn_id` — selects the Databricks connection.
- `gold_match_image_tag` — REQUIRED: the gate-passed build's sha (the
  evaluated artifact is the production artifact). Unset fails the run at
  render; `latest` works only as an explicit, provenance-warned override.
- `gold_match_image_pull_secret` — image pull secret name from Astronomer
  support (the GHCR package is private; the pull secret is required).
- `BRAINTRUST_API_KEY` — injected into the pod; the pinned prompt fails
  closed without it.
- `gold_match_aws_role_arn` / `gold_match_aws_external_id` — the
  GoodParty-account role the pod assumes for Bedrock and its trust
  ExternalId; the pod's own identity is Astronomer's.
"""

from __future__ import annotations

import logging
import re

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook
from airflow.sdk import Variable, dag, task
from include.custom_functions.databricks_utils import connect_from_conn_id
from include.custom_functions.gold_match_utils import (
    gold_match_pod_env,
    inflight_prod_builds,
    latest_scheduled_build_succeeded,
    new_quarantine_count,
    run_key_of,
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
# Bounds a hung pod without cutting a legitimate wave short: an all-states
# 8k-office cohort measures ~40 min (universe embedding dominates), and the pod
# must still leave the same-day audit its window before the 00:02Z build.
MATCH_EXECUTION_TIMEOUT = duration(hours=5)


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

    # retries=0: both checks are snapshots, so a retry minutes later could
    # admit a day the first attempt declined; a declined day is declined.
    @task.short_circuit(
        ignore_downstream_trigger_rules=False, retries=0, execution_timeout=duration(minutes=10)
    )
    def admission(ti=None) -> bool:
        """Decline the day cleanly, before anything is written, unless the
        latest SCHEDULED prod build succeeded (a red nightly means the
        universe and the marts are yesterday's, and if the matcher's own rows
        made it red the operator must remove them first) and no prod build is
        in flight (two builds on the same tables lost a mart write on
        2026-09-17). Fails CLOSED when dbt Cloud cannot be asked. Skips only
        its direct downstream and lets trigger rules propagate, so
        operator_signal still runs and reports the declined day."""
        reasons = []
        try:
            hook = DbtCloudHook("dbt_cloud")
            ok, latest = latest_scheduled_build_succeeded(hook)
            if not ok:
                reasons.append(f"the latest scheduled prod build did not succeed: {latest}")
            live = inflight_prod_builds(hook)
            if live:
                reasons.append("another prod build is in flight: " + ", ".join(live))
        except Exception as exc:
            reasons.append(f"dbt Cloud unreachable for the admission checks ({exc.__class__.__name__})")
        if reasons:
            reason = "; ".join(reasons)
            t_log.warning("publication declined at admission: %s", reason)
            ti.xcom_push(key="declined_reason", value=reason)
            return False
        return True

    @task(trigger_rule="all_done", execution_timeout=duration(minutes=15))
    def operator_signal(dag_run=None, ti=None) -> None:
        """Notification-only leaf: fails (so the DAG fails and the alert
        fires) on what a human must see. Runs on all_done so a declined day
        still signals; on a declined day nothing was written, so the reason is
        the whole story and the warehouse is not asked. Otherwise it reads the
        quarantine table, which the pod appends to for response-shape failures
        (the operator's own adjudication holds are excluded by their reason)."""
        declined = ti.xcom_pull(task_ids="admission", key="declined_reason")
        if declined:
            raise AirflowException(
                f"needs a human, nothing deleted: publication declined at admission ({declined}); "
                "nothing was written, tomorrow retries"
            )
        run_key = run_key_of(dag_run)
        conn = connect_from_conn_id()
        try:
            fresh = new_quarantine_count(conn, run_key)
        finally:
            conn.close()
        if fresh:
            raise AirflowException(
                f"needs a human, nothing deleted: {fresh} office(s) first entered quarantine this run"
            )
        t_log.info("nothing to signal")

    admission_task = admission()
    signal_task = operator_signal()

    admission_task >> match_pod >> signal_task


gold_match_daily()
