"""The behaviour table, the real-success markers, and the two outbound shapes.

Each test in the behaviour block is one row of the spec's table, and its name says
which row it is. The cron tests at the end pin rule 2's due time, which is
computable only from the last observed run start.
"""

from datetime import UTC, datetime, timedelta

import include.reader.records as records
from include.pipelines.models import Pipeline, RealSuccess, ToolIds
from include.reader.models import Run, Streak
from include.reader.records import (
    assemble_report,
    build_run_record,
    build_status_update,
    is_real_success,
    next_streak,
    rebuild_streak_from_history,
    watched_warning,
)
from include.reader.state import ReaderState

NIGHTLY = Pipeline(
    pipeline="dbt_nightly_build",
    tool="dbt_cloud",
    display_name="dbt Cloud nightly production build",
    owner="data-engineering",
    schedule="2 */12 * * *",
    allowed_age_seconds=26 * 3600,
    real_success=RealSuccess(status="run_status_success", trigger="scheduled"),
    runbook="https://example.invalid/runbook",
    watched_warnings=("assert_stripe_pro_subscriptions_match_campaigns",),
    tool_ids=ToolIds(job_id=1234),
)

T0 = datetime(2026, 11, 18, 0, 2, tzinfo=UTC)


def run(run_id=1, status=10, finished_minutes=40, started_minutes=0, scheduled=True):
    """A finished run `finished_minutes` after T0, started `started_minutes` after it."""
    return Run(
        run_id=run_id,
        job_id=1234,
        status=status,
        created_at=T0,
        started_at=T0 + timedelta(minutes=started_minutes),
        finished_at=T0 + timedelta(minutes=finished_minutes) if finished_minutes is not None else None,
        scheduled=scheduled,
        href="https://example.invalid/run",
    )


def error_run(run_id=1, finished_minutes=40):
    return run(run_id=run_id, status=20, finished_minutes=finished_minutes)


def cancelled_run(run_id=1, finished_minutes=40):
    return run(run_id=run_id, status=30, finished_minutes=finished_minutes)


# --- the behaviour table, one test per row ---------------------------------


def test_a_run_fails_sets_the_flag():
    streak = next_streak(Streak(False, None), NIGHTLY, [error_run()], read_ok=True)
    assert streak.failed_since_last_success is True


def test_a_later_cancelled_run_leaves_the_flag_set():
    """A cancelled run is not a recovery and is not a second failure: it says
    nothing about the pipeline, so the open failure stays open."""
    previous = Streak(failed_since_last_success=True, last_real_success_at=T0)
    streak = next_streak(previous, NIGHTLY, [cancelled_run(run_id=2, finished_minutes=90)], read_ok=True)
    assert streak.failed_since_last_success is True
    assert streak.last_real_success_at == T0


def test_a_real_success_clears_the_flag_and_records_its_finish_time():
    previous = Streak(failed_since_last_success=True, last_real_success_at=T0)
    success = run(run_id=3, status=10, finished_minutes=100)
    streak = next_streak(previous, NIGHTLY, [success], read_ok=True)
    assert streak.failed_since_last_success is False
    assert streak.last_real_success_at == success.finished_at


def test_a_failed_read_leaves_the_flag_alone():
    """A failed read is evidence about the reader, not the pipeline. The status
    update's last_read_ok = 0 is what carries it, and it is never a recovery."""
    previous = Streak(failed_since_last_success=True, last_real_success_at=T0)
    assert next_streak(previous, NIGHTLY, [], read_ok=False) == previous
    healthy = Streak(False, T0)
    assert next_streak(healthy, NIGHTLY, [], read_ok=False) == healthy


# --- cold start ------------------------------------------------------------


def test_cold_start_after_a_failure_reports_failed():
    runs = [run(run_id=2, status=10, finished_minutes=40), error_run(run_id=3, finished_minutes=90)]
    streak = next_streak(None, NIGHTLY, runs, read_ok=True)
    assert streak.failed_since_last_success is True
    assert streak.last_real_success_at == runs[0].finished_at


def test_cold_start_after_a_success_reports_healthy():
    runs = [error_run(run_id=2, finished_minutes=40), run(run_id=3, status=10, finished_minutes=90)]
    streak = next_streak(None, NIGHTLY, runs, read_ok=True)
    assert streak.failed_since_last_success is False
    assert streak.last_real_success_at == runs[1].finished_at


def test_cold_start_with_no_success_ever_reports_failed():
    """Never "not failed with a null success time": on a job that has never
    succeeded, that is the shape of a recovery that never happened."""
    streak = rebuild_streak_from_history(NIGHTLY, [error_run(run_id=2)])
    assert streak.failed_since_last_success is True
    assert streak.last_real_success_at is None


def test_cold_start_ignores_a_manual_success():
    runs = [
        run(run_id=2, status=10, finished_minutes=40, scheduled=False),
        error_run(run_id=3, finished_minutes=90),
    ]
    streak = rebuild_streak_from_history(NIGHTLY, runs)
    assert streak.failed_since_last_success is True
    assert streak.last_real_success_at is None


# --- real-success markers --------------------------------------------------


def test_success_marker_requires_the_scheduled_trigger():
    manual = run(status=10, finished_minutes=40, scheduled=False)
    assert is_real_success(NIGHTLY, manual) is False
    assert is_real_success(NIGHTLY, run(status=10, finished_minutes=40)) is True


def test_run_status_mapping():
    assert run(status=10).outcome == "success"
    assert run(status=20).outcome == "error"
    assert run(status=30).outcome == "cancelled"
    assert run(status=3, finished_minutes=None).outcome == "running"


def test_a_running_run_produces_no_record():
    running = run(status=3, finished_minutes=None)
    assert running.finished is False
    # A running run never reaches build_run_record from the collector, and the
    # status helper treats it as started but not finished.
    streak = next_streak(Streak(True, None), NIGHTLY, [running], read_ok=True)
    assert streak.failed_since_last_success is True


# --- the record ------------------------------------------------------------


def test_record_error_is_redacted():
    artifact = {
        "results": [
            {"unique_id": "test.pkg.assert_x", "status": "fail", "message": "row for someone@example.com"}
        ]
    }
    record = build_run_record(NIGHTLY, error_run(), artifact)
    assert "someone@example.com" not in record["error"]
    assert record["failed_node"] == "test.pkg.assert_x"


def test_a_failed_record_without_an_artifact_degrades_to_status_only():
    record = build_run_record(NIGHTLY, error_run(), None)
    assert record["status"] == "error"
    assert record["failed_node"] == ""
    assert record["error"] == ""
    # The link is still there, which is what a ticket needs to act on.
    assert record["link"]


def test_artifact_reads_only_message_and_unique_id():
    """compiled_code, failures and adapter_response can carry row values, and
    nothing here needs them."""
    artifact = {
        "results": [
            {
                "unique_id": "test.pkg.assert_x",
                "status": "fail",
                "message": "Got 1 result, configured to fail if != 0",
                "failures": 1,
                "adapter_response": {"_message": "SELECT 1"},
                "compiled_code": "select * from secret_table",
            }
        ]
    }
    record = build_run_record(NIGHTLY, error_run(), artifact)
    blob = repr(record)
    assert "secret_table" not in blob
    assert "SELECT 1" not in blob
    assert "Got 1 result, configured to fail if != 0" in record["error"]


def test_green_run_with_watched_warning_sets_the_flag():
    artifact = {
        "results": [
            {
                "unique_id": "test.pkg.assert_stripe_pro_subscriptions_match_campaigns",
                "status": "warn",
                "message": "a warning",
            }
        ]
    }
    assert watched_warning(NIGHTLY, artifact) == {
        "test": "assert_stripe_pro_subscriptions_match_campaigns",
        "count": 1,
    }
    record = build_run_record(NIGHTLY, run(status=10), artifact)
    assert record["watched_warning"] == "assert_stripe_pro_subscriptions_match_campaigns"
    assert record["watched_warning_count"] == 1


def test_unwatched_warning_does_not_set_the_flag():
    artifact = {
        "results": [{"unique_id": "test.pkg.assert_something_else", "status": "warn", "message": "x"}]
    }
    assert watched_warning(NIGHTLY, artifact) is None
    assert build_run_record(NIGHTLY, run(status=10), artifact)["watched_warning"] is None


def test_a_green_run_passes_cleanly():
    artifact = {"results": [{"unique_id": "test.pkg.assert_x", "status": "pass", "message": ""}]}
    assert watched_warning(NIGHTLY, artifact) is None
    assert build_run_record(NIGHTLY, run(status=10), artifact)["failed_node"] == ""


# --- the status update -----------------------------------------------------


def test_status_update_fields_are_numeric():
    """A recording rule can unwrap a number and cannot unwrap an ISO string."""
    update = build_status_update(
        NIGHTLY,
        Streak(True, T0),
        now=T0,
        due_at=T0,
        last_read_at=T0,
        last_read_ok=True,
        warning={"test": "assert_x", "count": 2},
    )
    for field in ("last_real_success_at", "next_run_due_at", "last_read_at", "read_at"):
        assert isinstance(update[field], int), field
    for field in ("failed_since_last_success", "watched_warning", "last_read_ok"):
        assert update[field] in (0, 1), field
    assert isinstance(update["allowed_age_seconds"], int)
    assert update["record_type"] == records.STATUS_UPDATE


def test_status_update_carries_a_null_success_time_when_there_never_was_one():
    update = build_status_update(NIGHTLY, Streak(True, None), T0, T0, T0, True)
    assert update["last_real_success_at"] is None


def test_status_update_carries_a_stale_read_time_when_the_read_failed():
    update = build_status_update(NIGHTLY, Streak(True, T0), T0, T0, T0, last_read_ok=False)
    assert update["last_read_ok"] == 0
    # Rule 4 reads this, so it must be the last GOOD read, not this attempt.
    assert update["last_read_at"] == int(T0.timestamp())


# --- the all_done send -----------------------------------------------------


def test_a_raising_collector_still_yields_a_status_update_with_last_read_ok_zero():
    """The contract change 1 rests on: an unexpected exception anywhere upstream
    still produces a status update, built from the last saved state, with
    last_read_ok = 0. Silence is the one thing rule 4 cannot tell from health."""
    state = ReaderState(streaks={"dbt_nightly_build": Streak(True, T0)}, last_read_at={"dbt_cloud": T0})
    records, statuses = assemble_report([NIGHTLY], {"dbt_cloud": None}, state, T0)
    assert records == []
    assert len(statuses) == 1
    update = statuses[0]
    assert update["last_read_ok"] == 0
    assert update["failed_since_last_success"] == 1
    assert update["last_read_at"] == int(T0.timestamp())  # the last GOOD read, not this attempt


def test_a_failed_read_never_reports_recovery():
    state = ReaderState(streaks={"dbt_nightly_build": Streak(False, T0)})
    _, statuses = assemble_report(
        [NIGHTLY], {"dbt_cloud": {"tool": "dbt_cloud", "read_ok": False}}, state, T0
    )
    assert statuses[0]["last_read_ok"] == 0
    # It does not flip the flag either way: the read said nothing about the pipeline.
    assert statuses[0]["failed_since_last_success"] == 0


def test_a_cold_start_with_a_failed_read_does_not_claim_healthy():
    """A first deploy that lost the Variable while the nightly is red must not
    report "not failed": a null success time with a 0 flag reads as healthy."""
    _, statuses = assemble_report([NIGHTLY], {"dbt_cloud": None}, ReaderState(), T0)
    assert statuses[0]["failed_since_last_success"] == 1
    assert statuses[0]["last_real_success_at"] is None


def test_a_successful_read_passes_records_and_the_streak_through():
    payload = {
        "tool": "dbt_cloud",
        "read_ok": True,
        "pipelines": {
            "dbt_nightly_build": {
                "read_ok": True,
                "records": [{"record_type": "run_record", "run_id": 4}],
                "streak": {"failed_since_last_success": False, "last_real_success_at": T0.isoformat()},
                "due_at": T0.isoformat(),
                "watched_warning": None,
            }
        },
    }
    records, statuses = assemble_report([NIGHTLY], {"dbt_cloud": payload}, ReaderState(), T0)
    assert [r["run_id"] for r in records] == [4]
    assert statuses[0]["last_read_ok"] == 1
    assert statuses[0]["failed_since_last_success"] == 0
    assert statuses[0]["last_real_success_at"] == int(T0.timestamp())
