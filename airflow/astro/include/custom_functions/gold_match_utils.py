"""Helpers for the gold-match daily DAG: pod environment, label gates,
run-row cleanup, and dbt Cloud control with terminal confirmation.

The matcher owns its tables; the DAG reads and repairs them over the
deployment's Databricks connection (the shared `conn_kwargs` accessor, so the
pod and the gate tasks cannot drift on which connection fields they need) and
drives the same dbt Cloud rebuild job the supervised runs use.
"""

import logging
from datetime import UTC, datetime, time, timedelta
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunStatus
from airflow.sdk import Variable
from include.custom_functions.databricks_utils import conn_kwargs, execute_with_retry

logger = logging.getLogger("airflow.task")

# The scheduled build: universe + every consumer. Mirrors backlog_run.py's
# DBT_CLOUD_REBUILD_JOB_ID in gold-match, which the supervised runs trigger.
GOLD_MATCH_REBUILD_JOB_ID = 70471823431462
# "dbt build on merge" (state:modified+ --full-refresh on every merge to main).
# A merge touching an upstream of the matcher marts during the loop's rebuild
# window puts two prod builds on the same tables (2026-09-17: two hours of
# overlap, one mart write lost to the collision), so admission checks it too.
ON_MERGE_BUILD_JOB_ID = 70471823431463
PROD_BUILD_JOB_IDS = (GOLD_MATCH_REBUILD_JOB_ID, ON_MERGE_BUILD_JOB_ID)
# The election-api sync reads the marts at this hour; the runbook's geometry
# section is the source of truth and this constant must move with it.
ELECTION_API_SYNC_HOUR_UTC = 22
# The plan's per-run memory cap killed three builds in their docs step AFTER
# the build had completed (2026-09-15/17), and the loop read the whole-run
# CANCELLED as a failure. The loop's runs never need a catalog; the setting
# is also off on the job, and this override keeps it off if that is toggled.
NO_DOCS_RUN_CONFIG = {"generate_docs_override": False}

# Success is judged from the run's results, not its status: a full-project
# build is red whenever ANY test in the project fails, and three of the loop's
# first four days were rolled back by failures outside the matcher's lineage
# (an unrelated staging test, a docs step). The rebuild is good when every
# model below built and every test below passed; anything else in the run is
# someone else's red. unique_id prefixes, matched exactly or before a dot so
# generic tests' hash suffixes do not need to be known here.
MATCHER_DEPENDENT_MODELS: dict[str, str] = {
    # The rule for membership: the model's OUTPUT carries the matcher's answer
    # (a district link, is_matched, or a voter count derived from one) and a
    # consumer reads it. Marts that merely join a listed mart for ids or names
    # (m_election_api__candidacy, office_holder) are not listed: their red is
    # not the matcher's, and the loop is what keeps THEIR red from blocking it.
    "model.goodparty_data_catalog.stg_model_predictions__llm_l2_br_match": "serves the newest answer per office",
    "model.goodparty_data_catalog.int__l2_district_universe": "the label gates read it; a dead label is one absent here",
    "model.goodparty_data_catalog.int__l2_br_match_pending_offices": "tomorrow's cohort; reads the results table directly",
    "model.goodparty_data_catalog.int__icp_offices": "is_matched and the district population feed lead sourcing",
    "model.goodparty_data_catalog.leads_win_candidacy": "carries the ICP office flags to lead sourcing",
    "model.goodparty_data_catalog.users_win_candidacy": "carries the ICP office flags to user analytics",
    "model.goodparty_data_catalog.int__zip_code_to_br_office": "the zip funnel",
    "model.goodparty_data_catalog.m_election_api__district": "the position mart's district ids and voter counts",
    "model.goodparty_data_catalog.m_election_api__position": "the product's position-to-district link",
    "model.goodparty_data_catalog.m_election_api__race": "carries the position link into races",
    "model.goodparty_data_catalog.m_election_api__zip_to_position": "the product's zip lookup",
    "model.goodparty_data_catalog.m_election_api__district_top_issues": "picks districts from the match",
    "model.goodparty_data_catalog.m_election_api__elected_official_support": "reads the position's icp_voter_count",
    "model.goodparty_data_catalog.int__serve_district_resolution": "Serve's district resolution reads is_matched",
    "model.goodparty_data_catalog.int__serve_block_coverage": "Serve coverage downstream of the resolution",
    "model.goodparty_data_catalog.people_served": "the Serve mart downstream of the resolution",
}
# Every error-severity singular test that reads a listed model, plus the
# staging model's own generic tests: a listed mart with its own hard test
# red is not a publication, whatever the rest of the project did. Generic
# tests carry dbt's argument-derived name and then a hash; the full name is
# pinned so an argument change fails loudly as "not in the build".
MATCHER_RELEVANT_TESTS: dict[str, str] = {
    "test.goodparty_data_catalog.not_null_stg_model_predictions__llm_l2_br_match_br_database_id": "staging identity",
    "test.goodparty_data_catalog.unique_stg_model_predictions__llm_l2_br_match_br_database_id": (
        "one served answer per office; a duplicate means the newest-row qualify broke"
    ),
    "test.goodparty_data_catalog.l2_district_tuple_exists_stg_model_predictions__llm_l2_br_match_attempted_at_"
    "timestamp_2026_01_26___l2_state__ref_int__l2_district_universe___district_name__district_type__state_postal_code": (
        "the staging label check (warn severity; the gates fail the run-scoped case)"
    ),
    "test.goodparty_data_catalog.assert_l2_br_match_staging_serves_newest_attempt": "staging must serve the run's rows",
    "test.goodparty_data_catalog.assert_l2_br_match_pending_offices_excludes_current_matches": (
        "a matched office must leave tomorrow's cohort"
    ),
    "test.goodparty_data_catalog.assert_llm_normalized_respellings_have_zip_coverage": "hard test on staging + the zip funnel",
    "test.goodparty_data_catalog.assert_override_positions_have_zip_coverage": "hard test on staging + the zip funnel",
    "test.goodparty_data_catalog.assert_icp_offices_district_population_null_share_by_type": "hard test on ICP offices",
    "test.goodparty_data_catalog.assert_icp_offices_voter_count_binds_l2": "hard test on ICP offices",
    "test.goodparty_data_catalog.assert_icp_offices_voter_count_null_share": "hard test on ICP offices",
    "test.goodparty_data_catalog.assert_icp_offices_voter_count_null_share_by_type": "hard test on ICP offices",
    "test.goodparty_data_catalog.assert_zip_to_br_office_one_district_type_per_br": "the zip funnel's shape",
    "test.goodparty_data_catalog.assert_zip_to_br_office_voters_in_zip_invariant": "the zip funnel's counts",
    "test.goodparty_data_catalog.assert_statewide_coverage_is_genuinely_statewide": "hard test on the zip funnel",
    "test.goodparty_data_catalog.assert_legislative_positions_have_zip_coverage": "hard test on the zip funnel + positions",
    "test.goodparty_data_catalog.assert_position_district_voter_coverage_floor": (
        "the voter-coverage floor; the DAG's gates deliberately delegate it to the build"
    ),
    "test.goodparty_data_catalog.assert_position_districts_are_not_voterless_duplicates": (
        "a rename must not bind positions to an empty duplicate district"
    ),
    "test.goodparty_data_catalog.assert_legislative_positions_resolve_to_a_populated_district": (
        "the product-facing link must carry voters"
    ),
    "test.goodparty_data_catalog.assert_override_seed_resolves_to_position_district": "hard test on positions",
    "test.goodparty_data_catalog.assert_race_election_code_matches_day_rule": "hard test on positions + races",
    "test.goodparty_data_catalog.assert_race_projection_two_way_inference_parity": "hard test on positions + races",
    "test.goodparty_data_catalog.mart_election_api_race_win_number_estimate_coverage_warn": (
        "hard test (error severity despite the name) on positions + races"
    ),
    "test.goodparty_data_catalog.assert_race_filing_date_overrides_applied": "hard test on races",
    "test.goodparty_data_catalog.assert_race_seats_match_ballotready_stage": "hard test on races",
    "test.goodparty_data_catalog.assert_race_slug_prefix_matches_place_slug": "hard test on races",
    "test.goodparty_data_catalog.assert_district_top_issues_seed_coverage": "hard test on district top issues",
    "test.goodparty_data_catalog.assert_override_districts_have_top_issues": "hard test on district top issues",
    "test.goodparty_data_catalog.assert_serve_district_resolution_coverage_floor": "Serve's coverage floor",
    "test.goodparty_data_catalog.assert_serve_statewide_binds_district_census_stats": "hard test on Serve resolution",
    "test.goodparty_data_catalog.assert_people_served_cohort_contract": "hard test on the Serve mart",
    "test.goodparty_data_catalog.assert_people_served_ordering_invariant": "hard test on the Serve mart",
}
# Passing outcomes per node kind in run_results.json; a warn-severity test
# reporting rows is a pass here because the gates own that decision.
_MODEL_OK = {"success"}
_TEST_OK = {"pass", "warn"}

# The matcher's tables, mirroring gold-match's l2_br_match_schema paths. The
# entry point writes the production catalog unconditionally, so these are
# constants rather than the catalog Variable the ER tables use.
RESULTS_TABLE = "goodparty_data_catalog.model_predictions.llm_l2_br_match_results"
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


def trigger_rebuild(hook: DbtCloudHook, cause: str) -> int:
    """Trigger the rebuild job with an operator-readable cause and no docs
    step. The cause string is an interface: a mislabeled trigger once got a
    healthy rebuild cancelled by a teammate acting reasonably on what it said."""
    response = hook.trigger_job_run(
        job_id=GOLD_MATCH_REBUILD_JOB_ID, cause=cause, additional_run_config=dict(NO_DOCS_RUN_CONFIG)
    )
    return int(response.json()["data"]["id"])


def _matches(unique_id: str, prefix: str) -> bool:
    return unique_id == prefix or unique_id.startswith(prefix + ".")


def rebuild_result_problems(run_results: dict[str, Any]) -> list[str]:
    """What is wrong with a rebuild, judged from its build step's run_results:
    every matcher-dependent model must have built and every matcher-relevant
    test must have passed; a listed node absent from the results counts as
    not built, so a narrowed selection cannot silently drop one. An empty
    list means the rebuild is good whatever the run's overall status."""
    status_by_id = {r.get("unique_id", ""): r.get("status", "") for r in run_results.get("results", [])}
    problems = []
    for prefix in MATCHER_DEPENDENT_MODELS:
        statuses = [st for uid, st in status_by_id.items() if _matches(uid, prefix)]
        if not statuses or any(st not in _MODEL_OK for st in statuses):
            problems.append(f"model {prefix.rsplit('.', 1)[-1]}: {statuses or 'not in the build'}")
    for prefix in MATCHER_RELEVANT_TESTS:
        statuses = [st for uid, st in status_by_id.items() if _matches(uid, prefix)]
        if not statuses or any(st not in _TEST_OK for st in statuses):
            problems.append(f"test {prefix.rsplit('.', 1)[-1]}: {statuses or 'not in the build'}")
    return problems


def _has_lineage(results: dict[str, Any]) -> bool:
    return any(
        _matches(r.get("unique_id", ""), p)
        for r in results.get("results", [])
        for p in MATCHER_DEPENDENT_MODELS
    )


def build_step_run_results(hook: DbtCloudHook, run_id: int) -> dict[str, Any]:
    """The `dbt build` step's run_results.json. dbt Cloud serves artifacts per
    step and defaults to the LAST step, which need not be the build, so the
    step index comes from the run's own step list when the API returns it;
    otherwise the last step and then the first dozen are tried in turn until
    one carries the matcher lineage. Fails loudly when none does: an
    unreadable rebuild is treated exactly like a failed one."""
    candidates: list[int | None] = []
    try:
        run_steps = (
            hook.get_job_run(run_id, include_related=["run_steps"]).json()["data"].get("run_steps") or []
        )
    except Exception:
        logger.warning("run_steps unavailable for dbt run %s; probing the steps' artifacts", run_id)
        run_steps = []
    candidates.extend(rs.get("index") for rs in run_steps if "dbt build" in str(rs.get("name", "")).lower())
    candidates.extend([None, *range(1, 13)])
    for step in candidates:
        try:
            results = hook.get_job_run_artifact(run_id, path="run_results.json", step=step).json()
        except Exception:
            continue
        if _has_lineage(results):
            return results
    raise AirflowException(f"dbt run {run_id}: no build results for the matcher lineage were readable")


def wait_for_rebuild(hook: DbtCloudHook, run_id: int, timeout_s: int) -> None:
    """Wait for the run to end, then judge it by its results. The hook raises
    only on timeout here (any terminal status is expected); a run that is
    ERROR on someone else's test but clean on the matcher lineage passes, and
    a SUCCESS run that skipped a matcher model does not."""
    hook.wait_for_job_run_status(
        run_id=run_id,
        expected_statuses=DbtCloudJobRunStatus.TERMINAL_STATUSES.value,
        check_interval=60,
        timeout=timeout_s,
    )
    status = DbtCloudJobRunStatus(hook.get_job_run_status(run_id)).name
    problems = rebuild_result_problems(build_step_run_results(hook, run_id))
    if problems:
        raise AirflowException(
            f"dbt run {run_id} ({status}) is not good for publication: " + "; ".join(problems)
        )
    logger.info(
        "dbt run %s (%s): every matcher-dependent model built and every matcher-relevant test passed",
        run_id,
        status,
    )


def trigger_rebuild_and_wait(hook: DbtCloudHook, cause: str, timeout_s: int = 10800) -> int:
    """The repair path: trigger, then wait and judge with the same criterion
    the post-write rebuild uses, so an unrelated red never reads as a failed
    repair."""
    run_id = trigger_rebuild(hook, cause)
    wait_for_rebuild(hook, run_id, timeout_s)
    return run_id


def inflight_prod_builds(hook: DbtCloudHook) -> list[str]:
    """Runs of the two prod-writing jobs that are queued, starting or running,
    as operator-readable labels. Newest runs only (one page, ordered by -id):
    a live run is always among the newest of its job."""
    live = {
        DbtCloudJobRunStatus.QUEUED.value,
        DbtCloudJobRunStatus.STARTING.value,
        DbtCloudJobRunStatus.RUNNING.value,
    }
    found: list[str] = []
    for job_id in PROD_BUILD_JOB_IDS:
        runs = hook.get_job_runs(
            payload={"job_definition_id": job_id, "order_by": "-id", "limit": 20}
        ).json()["data"]
        found.extend(
            f"job {job_id} run {r['id']} ({DbtCloudJobRunStatus(r['status']).name})"
            for r in runs
            if r["status"] in live
        )
    return found


def next_sync_deadline(now: datetime) -> datetime:
    """The next election-api sync at or after `now`, in UTC."""
    today = datetime.combine(now.astimezone(UTC).date(), time(ELECTION_API_SYNC_HOUR_UTC), tzinfo=UTC)
    return today if now <= today else today + timedelta(days=1)
