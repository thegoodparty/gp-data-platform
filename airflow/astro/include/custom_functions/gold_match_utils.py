"""Helpers for the gold-match daily DAG: pod environment, label gates,
run-row cleanup, and dbt Cloud control with terminal confirmation.

The matcher owns its tables; the DAG reads and repairs them over the
deployment's Databricks connection (the shared `conn_kwargs` accessor, so the
pod and the gate tasks cannot drift on which connection fields they need) and
drives the same dbt Cloud rebuild job the supervised runs use.
"""

import logging
from datetime import datetime
from typing import Any

from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.sdk import Variable
from include.custom_functions.databricks_utils import conn_kwargs, execute_with_retry

logger = logging.getLogger("airflow.task")

# The scheduled build: universe + every consumer. Mirrors backlog_run.py's
# DBT_CLOUD_REBUILD_JOB_ID in gold-match, which the supervised runs trigger.
GOLD_MATCH_REBUILD_JOB_ID = 70471823431462

# The matcher's tables, mirroring gold-match's l2_br_match_schema paths. The
# entry point writes the production catalog unconditionally, so these are
# constants rather than the catalog Variable the ER tables use.
RESULTS_TABLE = "goodparty_data_catalog.model_predictions.llm_l2_br_match_results"
QUARANTINE_TABLE = "goodparty_data_catalog.model_predictions.llm_l2_br_match_quarantine"

BRAINTRUST_VARIABLE = "BRAINTRUST_API_KEY"


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
    # The gold-match client always authenticates with the SDK's default OAuth
    # scopes. The deployment-wide `databricks_scopes` Variable exists to match
    # a service-principal secret minted with NARROWER scopes (see conn_kwargs),
    # so when it is set, the pod's default-scopes token request will be
    # refused. Refusing here, before a pod is paid for, beats a generic auth
    # failure inside it; lift this once the client honors scopes
    # (gold-match follow-up).
    if fields.get("scopes"):
        raise ValueError(
            "the databricks_scopes Variable is set, meaning the service principal's secret "
            "carries narrowed scopes, but the gold-match client always requests the SDK "
            "default and would fail to authenticate inside the pod; add scopes support to "
            "the client, or run this pipeline against a secret allowing the default scopes"
        )
    host = fields["host"].removeprefix("https://").removeprefix("http://").rstrip("/")
    return {
        "DATABRICKS_SERVER_HOSTNAME": host,
        "DATABRICKS_HTTP_PATH": fields["http_path"],
        "DATABRICKS_CLIENT_ID": fields["client_id"],
        "DATABRICKS_CLIENT_SECRET": fields["client_secret"],
        # Fail here (Variable.get raises) rather than inside the paid pod: the
        # pinned prompt refuses to load without the key anyway.
        "BRAINTRUST_API_KEY": Variable.get(BRAINTRUST_VARIABLE),
        "ENVIRONMENT": "production",
    }


# Mirrors the gold-match run-audit's Step 1 label checks: matched tuples
# against the current district universe. The 2026-01-26 baseline run predates
# the universe contract and is excluded for the same reason the staging label
# test excludes it. The literal pins its UTC offset because a bare timestamp
# reads in the warehouse SESSION timezone, which nothing here pins (the
# TestTimestampLiteralsPreserveOffset precedent in gold-match).
_GLOBAL_DEAD_SQL = """
    with label_check_tuples as (
        select distinct l2_state, l2_district_type, l2_district_name
        from goodparty_data_catalog.dbt.stg_model_predictions__llm_l2_br_match
        where l2_district_name is not null and attempted_at <> timestamp'2026-01-26 00:00:00+00:00'
    )
    select count(*)
    from label_check_tuples
    left join goodparty_data_catalog.dbt.int__l2_district_universe as universe
        on universe.state_postal_code = label_check_tuples.l2_state
        and universe.district_type = label_check_tuples.l2_district_type
        and universe.district_name = label_check_tuples.l2_district_name
    where universe.state_postal_code is null
"""

_RUN_DEAD_SQL = f"""
    with run_rows as (
        select distinct l2_state, l2_district_type, l2_district_name
        from {RESULTS_TABLE}
        where attempted_at = :run_key and l2_district_name is not null
    )
    select count(*)
    from run_rows
    left join goodparty_data_catalog.dbt.int__l2_district_universe as universe
        on universe.state_postal_code = run_rows.l2_state
        and universe.district_type = run_rows.l2_district_type
        and universe.district_name = run_rows.l2_district_name
    where universe.state_postal_code is null
"""


def run_gate_queries(conn: Any, run_key: datetime) -> dict[str, int]:
    """The two label metrics. Run-scoped nonzero means THIS run matched a
    now-dead tuple (destroying the run is the remedy); global-with-run-zero is
    an older run's dead tuple (repair at source, never by deleting this run).
    The coverage floor is deliberately absent: the rebuild job runs it at
    error severity, so a breach fails the rebuild task instead."""
    cursor = conn.cursor()
    try:
        execute_with_retry(cursor, _RUN_DEAD_SQL, {"run_key": run_key})
        run_scoped = int(cursor.fetchone()[0])
        execute_with_retry(cursor, _GLOBAL_DEAD_SQL)
        global_dead = int(cursor.fetchone()[0])
    finally:
        cursor.close()
    return {"run_scoped_dead": run_scoped, "global_dead": global_dead}


def new_quarantine_count(conn: Any, run_key: datetime) -> int:
    """Offices that FIRST entered quarantine on this run: inserts stamp
    first_failed_at with the run key exactly, while backoff re-fails only
    re-stamp last_failed_at and stay silent. Equality rather than an interval
    so a manual trigger's sub-second offsets cannot hide the run's own rows."""
    cursor = conn.cursor()
    try:
        execute_with_retry(
            cursor,
            f"select count(*) from {QUARANTINE_TABLE} where first_failed_at = :run_key",
            {"run_key": run_key},
        )
        return int(cursor.fetchone()[0])
    finally:
        cursor.close()


def delete_run_rows(conn: Any, run_key: datetime) -> int:
    """Delete the run's rows by key. No expected_count on purpose: unlike the
    supervised rollback, cleanup can fire before any count exists (a pod dead
    mid-write), so the honest contract is delete-whatever-landed, with the
    pre-count logged as the audit line."""
    cursor = conn.cursor()
    try:
        execute_with_retry(
            cursor,
            f"select count(*) from {RESULTS_TABLE} where attempted_at = :run_key",
            {"run_key": run_key},
        )
        count = int(cursor.fetchone()[0])
        execute_with_retry(
            cursor,
            f"delete from {RESULTS_TABLE} where attempted_at = :run_key",
            {"run_key": run_key},
        )
    finally:
        cursor.close()
    logger.info("Deleted %d result row(s) under run key %s", count, run_key.isoformat())
    return count


def cancel_dbt_run_and_confirm(hook: DbtCloudHook, run_id: int, timeout_s: int = 300) -> None:
    """Cancel a live dbt run and REQUIRE terminal confirmation: the provider's
    own kill path only warns when cancel or confirm fails, and cleanup must
    not delete-and-rebuild while a cancelled rebuild could still be writing.
    Any terminal state confirms (a run that finished just before the cancel is
    equally safe); the wait raises on timeout. The cancel POST itself is
    best-effort: on the common gates-failure path the rebuild already
    SUCCEEDED, and an API objection to cancelling a terminal run must not
    kill cleanup before the delete — the contract is terminal-confirmed,
    not cancel-succeeded."""
    try:
        hook.cancel_job_run(run_id)
    except Exception:
        logger.warning("cancel_job_run(%s) raised; confirming terminal state anyway", run_id, exc_info=True)
    hook.wait_for_job_run_status(
        run_id=run_id,
        expected_statuses=DbtCloudJobRunStatus.TERMINAL_STATUSES.value,
        check_interval=10,
        timeout=timeout_s,
    )


def trigger_rebuild_and_wait(hook: DbtCloudHook, cause: str, timeout_s: int = 10800) -> int:
    """Trigger the rebuild job with an operator-readable cause and wait for
    SUCCESS (the hook raises on a failed run and on timeout). The cause string
    is an interface: a mislabeled trigger once got a healthy rebuild cancelled
    by a teammate acting reasonably on what it said."""
    run_id = int(hook.trigger_job_run(job_id=GOLD_MATCH_REBUILD_JOB_ID, cause=cause).json()["data"]["id"])
    hook.wait_for_job_run_status(
        run_id=run_id,
        expected_statuses=DbtCloudJobRunStatus.SUCCESS.value,
        check_interval=60,
        timeout=timeout_s,
    )
    return run_id
