"""Helpers for the gold-match daily DAG: pod environment, the admission
reads against dbt Cloud, and the operator signal's quarantine read.

The matcher owns its tables; the DAG reads them over the deployment's
Databricks connection (the shared `conn_kwargs` accessor, so the pod and the
signal task cannot drift on which connection fields they need). It triggers no
dbt build: the scheduled nightly publishes what the pod writes.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.sdk import Variable
from include.custom_functions.databricks_utils import conn_kwargs, execute_with_retry

# The scheduled full prod build (00:02 and 12:02 UTC): universe + every
# consumer. The loop rides it instead of triggering its own; admission asks
# whether its latest scheduled run succeeded and whether one is in flight.
SCHEDULED_BUILD_JOB_ID = 70471823431462
# "dbt build on merge" (state:modified+ --full-refresh on every merge to main).
# A merge touching an upstream of the matcher marts while the pod writes puts
# a build on the same tables; admission checks it too.
ON_MERGE_BUILD_JOB_ID = 70471823431463
PROD_BUILD_JOB_IDS = (SCHEDULED_BUILD_JOB_ID, ON_MERGE_BUILD_JOB_ID)
_LIVE_STATUSES = {
    DbtCloudJobRunStatus.QUEUED.value,
    DbtCloudJobRunStatus.STARTING.value,
    DbtCloudJobRunStatus.RUNNING.value,
}
# The prod build runs twice a day, so a scheduled success older than this is
# not "the latest nightly": the schedule was disabled, missed, or never created,
# and the universe the pod would match against is stale.
SCHEDULED_BUILD_MAX_AGE = timedelta(hours=24)

# The matcher's quarantine table, mirroring gold-match's l2_br_match_schema
# path. The entry point writes the production catalog unconditionally, so this
# is a constant rather than the catalog Variable the ER tables use.
QUARANTINE_TABLE = "goodparty_data_catalog.model_predictions.llm_l2_br_match_quarantine"
# A hand-written `held` row for an office whose match the run audit adjudicated
# WRONG on the pinned build; released by hand when the quality lane re-pins.
# The pod's own rows carry the client's response-shape reason instead.
QUARANTINE_REASON_ADJUDICATED_WRONG = "adjudicated_wrong"

BRAINTRUST_VARIABLE = "BRAINTRUST_API_KEY"
# The GoodParty-account role the pod assumes for Bedrock, and the trust
# policy's ExternalId: the pod's own identity is Astronomer's and cannot
# hold the grant (gold_match_iam.tf in gp-terraform-dataplatform).
AWS_ROLE_ARN_VARIABLE = "gold_match_aws_role_arn"
AWS_EXTERNAL_ID_VARIABLE = "gold_match_aws_external_id"


def run_key_of(dag_run: Any) -> datetime:
    """The run's identity everywhere: the pod's --run-key, the delete, the
    gates. The entry point truncates attempted_at to whole seconds when
    writing, so every warehouse comparison here must truncate identically or
    match nothing."""
    return dag_run.start_date.replace(microsecond=0)


def gold_match_pod_env() -> dict[str, str]:
    """The env the gold-match container authenticates with.

    Built on the same connection accessor as the gate tasks. The gold-match
    client reads DATABRICKS_SERVER_HOSTNAME (a bare host), not the
    DATABRICKS_HOST form matcha's pod takes, so the mapping lives here in one
    place. ENVIRONMENT tags the pod's Braintrust spans and logs; the entry
    point's tables are production-only by construction, so it is a literal.
    """
    fields = conn_kwargs()
    host = fields["host"].removeprefix("https://").removeprefix("http://").rstrip("/")
    env = {
        "DATABRICKS_SERVER_HOSTNAME": host,
        "DATABRICKS_HTTP_PATH": fields["http_path"],
        "DATABRICKS_CLIENT_ID": fields["client_id"],
        "DATABRICKS_CLIENT_SECRET": fields["client_secret"],
        # Fail here (Variable.get raises) rather than inside the paid pod: the
        # pinned prompt refuses to load without the key anyway.
        "BRAINTRUST_API_KEY": Variable.get(BRAINTRUST_VARIABLE),
        # Same fail-here posture: without the role the pod would call Bedrock as
        # Astronomer's identity and be refused after the (paid) universe build began.
        "GOLD_MATCH_AWS_ROLE_ARN": Variable.get(AWS_ROLE_ARN_VARIABLE),
        "GOLD_MATCH_AWS_EXTERNAL_ID": Variable.get(AWS_EXTERNAL_ID_VARIABLE),
        "ENVIRONMENT": "production",
    }
    # The deployment-wide `databricks_scopes` Variable mirrors the scopes the
    # service principal's secret was minted with (see conn_kwargs); the client
    # requests the SDK default unless told otherwise, so the pod's token
    # exchange is refused without this forward. Absent when unset: empty means
    # the default on both sides.
    if fields.get("scopes"):
        env["DATABRICKS_SCOPES"] = ",".join(fields["scopes"])
    return env


def new_quarantine_count(conn: Any, run_key: datetime) -> int:
    """Offices that FIRST entered quarantine on this run: inserts stamp
    first_failed_at with the run key exactly, while backoff re-fails only
    re-stamp last_failed_at and stay silent. Equality rather than an interval
    so a manual trigger's sub-second offsets cannot hide the run's own rows.
    Hand-written adjudication holds are stamped with the audited run's key by
    convention and are the operator's own doing, so they never raise the
    alarm meant for the pod's response-shape failures."""
    cursor = conn.cursor()
    try:
        execute_with_retry(
            cursor,
            f"select count(*) from {QUARANTINE_TABLE} "
            "where first_failed_at = :run_key and reason_code <> :adjudicated",
            {"run_key": run_key, "adjudicated": QUARANTINE_REASON_ADJUDICATED_WRONG},
        )
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def _newest_runs(hook: DbtCloudHook, job_id: int, limit: int = 20) -> list[dict[str, Any]]:
    """The newest runs of one job, newest first, with their trigger attached.
    One page: a live or latest run is always among the newest."""
    payload = {"job_definition_id": job_id, "order_by": "-id", "limit": limit, "include_related": ["trigger"]}
    return list(hook.get_job_runs(payload=payload).json()["data"])


def _is_scheduled(run: dict[str, Any]) -> bool:
    """dbt Cloud marks a scheduled run by its trigger's cause; API-triggered
    runs (a hand re-run, another DAG) say so instead. Without trigger data
    every run counts, which is the safe direction: an unknown latest run that
    failed still declines."""
    trigger = run.get("trigger") or {}
    cause = str(trigger.get("cause") or "")
    return "schedul" in cause.lower() if cause else True


def latest_scheduled_build_succeeded(hook: DbtCloudHook, now: datetime | None = None) -> tuple[bool, str]:
    """Whether the newest SCHEDULED run of the prod build ended SUCCESS within
    the last day, with an operator-readable label of that run. No scheduled
    run in the newest page, or one older than SCHEDULED_BUILD_MAX_AGE, or one
    without a finish time, is a decline: the universe's freshness is unknown."""
    now = now or datetime.now(UTC)
    for run in _newest_runs(hook, SCHEDULED_BUILD_JOB_ID):
        if not _is_scheduled(run):
            continue
        status = DbtCloudJobRunStatus(run["status"]).name
        finished = str(run.get("finished_at") or "")
        label = f"job {SCHEDULED_BUILD_JOB_ID} run {run['id']} ({status}, finished {finished or 'unknown'})"
        if run["status"] != DbtCloudJobRunStatus.SUCCESS.value:
            return False, label
        try:
            finished_at = datetime.fromisoformat(finished.replace("Z", "+00:00"))
        except ValueError:
            return False, label
        if finished_at.tzinfo is None:
            finished_at = finished_at.replace(tzinfo=UTC)
        return now - finished_at <= SCHEDULED_BUILD_MAX_AGE, label
    return False, f"no scheduled run of job {SCHEDULED_BUILD_JOB_ID} among its newest runs"


def inflight_prod_builds(hook: DbtCloudHook) -> list[str]:
    """Runs of the two prod-writing jobs that are queued, starting or running,
    as operator-readable labels."""
    found: list[str] = []
    for job_id in PROD_BUILD_JOB_IDS:
        found.extend(
            f"job {job_id} run {r['id']} ({DbtCloudJobRunStatus(r['status']).name})"
            for r in _newest_runs(hook, job_id)
            if r["status"] in _LIVE_STATUSES
        )
    return found
