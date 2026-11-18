"""The dbt Cloud collector: records, the streak, the due time and the error contract.

Every fixture is invented. Nothing here reaches dbt Cloud, the metastore or a
credential.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from include.pipelines.models import Pipeline, RealSuccess, ToolIds
from include.reader.collect_dbt import collect, pick_step
from include.reader.dbt_cloud import ArtifactUnavailable
from include.reader.models import Run, Step, Streak
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
NOW = datetime(2026, 11, 18, 6, 0, tzinfo=UTC)


def run(run_id=1, status=10, created=T0, started=T0, finished=T0 + timedelta(minutes=40), scheduled=True):
    return Run(
        run_id=run_id,
        job_id=1234,
        status=status,
        created_at=created,
        started_at=started,
        finished_at=finished,
        scheduled=scheduled,
        href=f"https://example.invalid/runs/{run_id}",
    )


STEPS = [
    Step(index=1, name="Invoke dbt with `dbt deps`", status=10),
    Step(index=2, name="Invoke dbt with `dbt build`", status=10),
    Step(index=3, name="Invoke dbt with `dbt docs generate`", status=10),
]


def client(runs=None, unfinished=None, steps=None, artifact=None, raises=None):
    fake = MagicMock()
    if raises is not None:
        fake.list_runs.side_effect = raises
    else:
        fake.list_runs.return_value = runs or []
    fake.first_unfinished.return_value = unfinished
    fake.list_steps.return_value = steps if steps is not None else STEPS
    if isinstance(artifact, Exception):
        fake.fetch_run_results.side_effect = artifact
    else:
        fake.fetch_run_results.return_value = artifact or {"results": []}
    return fake


FAILED_ARTIFACT = {
    "results": [
        {"unique_id": "model.pkg.stg_x", "status": "success", "message": ""},
        {
            "unique_id": "test.pkg.assert_no_duplicate_paid_p2p_sends",
            "status": "fail",
            "message": "Got 1 result, configured to fail if != 0",
        },
    ]
}


def test_a_failed_run_yields_one_record_with_the_failed_step_and_node():
    failed_steps = [
        STEPS[0],
        Step(index=2, name="Invoke dbt with `dbt build`", status=20),
        Step(index=3, name="Invoke dbt with `dbt docs generate`", status=10),
    ]
    fake = client(runs=[run(status=20)], steps=failed_steps, artifact=FAILED_ARTIFACT)
    result = collect(NIGHTLY, fake, ReaderState(), NOW)
    assert result.read_ok is True
    assert len(result.records) == 1
    record = result.records[0]
    assert record["status"] == "error"
    assert record["failed_node"] == "test.pkg.assert_no_duplicate_paid_p2p_sends"
    assert "Got 1 result" in record["error"]
    assert record["run_id"] == 1
    # The failed step's artifact, not the last step's (docs generate looks green).
    assert fake.fetch_run_results.call_args.args[1] == 2


def test_pick_step_prefers_the_failed_step_then_the_dbt_build_step():
    assert (
        pick_step(
            [Step(1, "Invoke dbt with `dbt build`", 10), Step(2, "Invoke dbt with `dbt docs generate`", 10)],
            run(),
        )
        == 1
    )
    assert (
        pick_step(
            [Step(1, "Invoke dbt with `dbt deps`", 10), Step(2, "Invoke dbt with `dbt build`", 20)],
            run(status=20),
        )
        == 2
    )
    assert pick_step([], run()) is None


def test_a_green_run_is_still_read_for_a_watched_warning():
    """The artifact is fetched on green runs too: a watched warning only exists
    there, and nothing else reads those two finance checks."""
    green_with_warning = {
        "results": [
            {
                "unique_id": "test.pkg.assert_stripe_pro_subscriptions_match_campaigns",
                "status": "warn",
                "message": "a warning",
            }
        ]
    }
    fake = client(runs=[run(status=10)], artifact=green_with_warning)
    result = collect(NIGHTLY, fake, ReaderState(), NOW)
    assert fake.fetch_run_results.called
    assert result.watched_warning == {"test": "assert_stripe_pro_subscriptions_match_campaigns", "count": 1}
    assert result.records[0]["watched_warning"] == "assert_stripe_pro_subscriptions_match_campaigns"


def test_artifact_failure_degrades_to_a_status_only_record():
    fake = client(runs=[run(status=20)], artifact=ArtifactUnavailable("404"))
    result = collect(NIGHTLY, fake, ReaderState(), NOW)
    assert result.read_ok is True
    assert result.records[0]["status"] == "error"
    assert result.records[0]["failed_node"] == ""
    assert result.records[0]["link"]


def test_a_client_error_returns_read_ok_false_and_no_records():
    fake = client(raises=OSError("connection refused"))
    result = collect(NIGHTLY, fake, ReaderState(), NOW)
    assert result.read_ok is False
    assert result.records == []
    assert result.streak is None


def test_a_raising_client_returns_read_ok_false():
    """An unexpected exception must not escape the task: the DAG still sends the
    status update with last_read_ok = 0."""
    fake = client(raises=RuntimeError("auth is a bad token"))
    result = collect(NIGHTLY, fake, ReaderState(streaks={}), NOW)
    assert result.read_ok is False


def test_a_failed_read_keeps_the_saved_streak():
    previous = Streak(True, T0)
    fake = client(raises=OSError("down"))
    result = collect(NIGHTLY, fake, ReaderState(streaks={NIGHTLY.pipeline: previous}), NOW)
    assert result.streak == previous


def test_an_already_seen_run_is_not_resent():
    state = ReaderState(recent_run_ids={"dbt_cloud": ["1"]}, streaks={NIGHTLY.pipeline: Streak(False, T0)})
    result = collect(NIGHTLY, client(runs=[run(run_id=1)]), state, NOW)
    assert result.records == []


def test_the_cursor_is_the_newest_finish():
    state = ReaderState(streaks={NIGHTLY.pipeline: Streak(False, T0)})
    older = run(run_id=2, finished=T0 + timedelta(minutes=40))
    later_created_earlier_finish = run(
        run_id=3, created=T0 + timedelta(hours=1), finished=T0 + timedelta(minutes=30)
    )
    result = collect(NIGHTLY, client(runs=[later_created_earlier_finish, older]), state, NOW)
    assert result.cursor == older.finished_at


def test_a_cold_start_walks_the_history_for_the_streak():
    runs = [
        run(run_id=1, finished=T0 + timedelta(minutes=40)),
        run(run_id=2, status=20, created=T0 + timedelta(hours=12), finished=T0 + timedelta(hours=13)),
    ]
    result = collect(NIGHTLY, client(runs=runs), ReaderState(), NOW)
    assert result.streak is not None
    assert result.streak.failed_since_last_success is True
    assert result.streak.last_real_success_at == runs[0].finished_at


def test_missed_run_leaves_due_at_in_the_past():
    """Rule 2 compares the due time to now. A run that never started leaves the
    last observed start behind, so the due time is behind too and the rule fires."""
    stale = run(
        run_id=1,
        created=T0 - timedelta(days=2),
        started=T0 - timedelta(days=2),
        finished=T0 - timedelta(days=2) + timedelta(minutes=40),
    )
    result = collect(NIGHTLY, client(runs=[stale]), ReaderState(), NOW)
    assert result.due_at is not None
    assert result.due_at < NOW


def test_long_running_nightly_keeps_due_at_ahead():
    """A started-but-unfinished run counts as started, so a nightly that runs long
    does not look like a missed run while it is still running."""
    started = datetime(2026, 11, 18, 12, 2, tzinfo=UTC)
    in_flight = run(run_id=9, status=3, started=started, finished=None)
    result = collect(NIGHTLY, client(runs=[], unfinished=in_flight), ReaderState(), NOW + timedelta(hours=7))
    assert result.due_at is not None
    assert result.due_at > NOW + timedelta(hours=7)
