"""Builds the two outbound shapes from raw tool data: the run record and the
per-pipeline status update. Pure functions, no I/O, so every rule here is a unit
test.

Two things this module is careful about:

- `next_streak` is the behaviour table. A failure followed by a cancelled or
  skipped run stays failed, because the only thing that clears the flag is a run
  that counts as a real success.
- Every value that leaves is on an allowlist. From `run_results.json` only
  `message` and `unique_id` are read; `compiled_code`, `failures` and
  `adapter_response` can carry row values and nothing here needs them.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.timetables.base import TimeRestriction
from airflow.timetables.trigger import CronTriggerTimetable
from include.pipelines.models import Pipeline
from include.reader.models import Run, Streak
from include.reader.redact import redact_error
from pendulum import instance as pendulum_instance

STATUS_UPDATE = "status_update"
RUN_RECORD = "run_record"

# Only these keys ever leave the reader from a run_results.json result.
RESULT_TEST_KEY = "unique_id"
RESULT_MESSAGE_KEY = "message"
RESULT_STATUS_KEY = "status"


def is_real_success(pipeline: Pipeline, run: Run) -> bool:
    """Whether this run counts as recovery for this pipeline.

    dbt Cloud's `run_status_success` plus the pipeline's trigger. A manual run may
    be a partial or targeted build, so a pipeline that names `scheduled` is not
    recovered by one.
    """
    if run.outcome != "success":
        return False
    wanted = pipeline.real_success.trigger
    if wanted is None:
        return True
    if wanted == "scheduled":
        return run.scheduled
    if wanted == "manual":
        return not run.scheduled
    # on_merge and anything future: the tool must say so, and dbt Cloud's own
    # trigger causes are the only evidence available, so an unmarked run does not count.
    return False


def _fold(previous: Streak, pipeline: Pipeline, runs: list[Run]) -> Streak:
    """Apply terminal runs oldest first. Cancelled, skipped and running runs change nothing."""
    streak = previous
    for run in sorted(runs, key=lambda r: (r.finished_at is None, r.finished_at or datetime.min)):
        if not run.finished:
            continue
        if run.outcome == "error":
            streak = Streak(failed_since_last_success=True, last_real_success_at=streak.last_real_success_at)
        elif is_real_success(pipeline, run):
            # The finish time, not the start: rule 3 measures age from when the
            # build actually completed.
            streak = Streak(failed_since_last_success=False, last_real_success_at=run.finished_at)
    return streak


def rebuild_streak_from_history(pipeline: Pipeline, runs: list[Run]) -> Streak:
    """The streak implied by the runs themselves, for a cold start.

    A cold start must never assume healthy: this ships while the nightly is red on
    about half its runs, and a lost or hand-edited Variable would otherwise turn
    every open failure into a recovery that never happened.
    """
    return _fold(Streak(failed_since_last_success=False, last_real_success_at=None), pipeline, runs)


def next_streak(previous: Streak | None, pipeline: Pipeline, runs: list[Run], read_ok: bool) -> Streak:
    """Row by row, the spec's behaviour table.

    - a run fails: failed = true
    - a retry, a skip, a cancel or a rehearsal: unchanged
    - a real success finishes: failed = false, last success = its finish time
    - the reader cannot read the tool: the flag is left exactly as it was, because a
      failed read is evidence about the reader, not about the pipeline. Rule 4 reads
      `last_read_at` for that, and nothing is reported as recovery.
    """
    if not read_ok:
        # No state and a failed read leaves the flag unknown; the status update's
        # last_read_ok = 0 is what carries that, and it is never a recovery.
        return previous if previous is not None else Streak(False, None)
    if previous is None:
        return rebuild_streak_from_history(pipeline, runs)
    return _fold(previous, pipeline, runs)


def watched_warning(pipeline: Pipeline, artifact: dict[str, Any] | None) -> dict[str, Any] | None:
    """The first watched test whose results warn, with how many of its results warned.

    Watched warnings only exist on green runs, which is why the artifact is read on
    every finished run and not only on failures.
    """
    if not pipeline.watched_warnings or not artifact:
        return None
    counts: dict[str, int] = {}
    for result in artifact.get("results") or []:
        unique_id = str(result.get(RESULT_TEST_KEY) or "")
        name = unique_id.rsplit(".", 1)[-1]
        if name not in pipeline.watched_warnings:
            continue
        if str(result.get(RESULT_STATUS_KEY) or "") != "warn":
            continue
        counts[name] = counts.get(name, 0) + 1
    if not counts:
        return None
    name = next(n for n in pipeline.watched_warnings if n in counts)
    return {"test": name, "count": counts[name]}


def failed_node_and_error(artifact: dict[str, Any] | None) -> tuple[str, str]:
    """The failing node and its one redacted message, from a step's run_results.json.

    Reads only `unique_id` and `message`. A missing artifact degrades to empty
    strings so the record is status-only rather than the run failing.
    """
    if not artifact:
        return "", ""
    for result in artifact.get("results") or []:
        if str(result.get(RESULT_STATUS_KEY) or "") not in {"fail", "error"}:
            continue
        node = str(result.get(RESULT_TEST_KEY) or "")
        return node, redact_error(result.get(RESULT_MESSAGE_KEY))
    return "", ""


def build_run_record(pipeline: Pipeline, run: Run, artifact: dict[str, Any] | None = None) -> dict[str, Any]:
    """One short record per finished run, from the field allowlist only."""
    failed_node, error = failed_node_and_error(artifact)
    warning = watched_warning(pipeline, artifact)
    duration = None
    if run.started_at and run.finished_at:
        duration = int((run.finished_at - run.started_at).total_seconds())
    return {
        "record_type": RUN_RECORD,
        "pipeline": pipeline.pipeline,
        "tool": pipeline.tool,
        "display_name": pipeline.display_name,
        "owner": pipeline.owner,
        "run_id": run.run_id,
        "status": run.outcome,
        "scheduled": run.scheduled,
        "started_at": _epoch(run.started_at),
        "finished_at": _epoch(run.finished_at),
        "duration_seconds": duration,
        "failed_node": failed_node,
        "error": error,
        "watched_warning": warning["test"] if warning else None,
        "watched_warning_count": warning["count"] if warning else 0,
        "link": run.href,
        "runbook": pipeline.runbook,
    }


def next_run_due(schedule: str | None, last_started_at: datetime | None) -> datetime | None:
    """The next cron occurrence after the last observed run start.

    Computed from the last start rather than from now, because from now it is
    always in the future and rule 2 could never fire. Airflow's own timetable is
    used so the answer matches when Airflow will actually schedule the run; the
    one-second nudge makes `earliest` exclusive, which yields the next occurrence
    even when the anchor sits exactly on a cron time.
    """
    if not schedule or last_started_at is None:
        return None
    timetable = CronTriggerTimetable(schedule, timezone="UTC")
    # TimeRestriction takes pendulum DateTimes, so the anchor is converted rather
    # than passed as a plain datetime.
    anchor = pendulum_instance(last_started_at.astimezone(UTC) + timedelta(seconds=1))
    info = timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(earliest=anchor, latest=None, catchup=True),
    )
    return datetime.fromisoformat(info.run_after.isoformat()) if info is not None else None


def _epoch(value: datetime | None) -> int | None:
    return int(value.timestamp()) if value is not None else None


def build_status_update(
    pipeline: Pipeline,
    streak: Streak,
    now: datetime,
    due_at: datetime,
    last_read_at: datetime | None,
    last_read_ok: bool,
    warning: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One status update per pipeline per reader run.

    Every time is an int epoch second and every flag is 0 or 1: a recording rule
    can `unwrap` a number and cannot `unwrap` an ISO string.
    """
    return {
        "record_type": STATUS_UPDATE,
        "pipeline": pipeline.pipeline,
        "tool": pipeline.tool,
        "display_name": pipeline.display_name,
        "owner": pipeline.owner,
        "failed_since_last_success": 1 if streak.failed_since_last_success else 0,
        "watched_warning": 1 if warning else 0,
        "watched_warning_test": warning["test"] if warning else "",
        "last_real_success_at": _epoch(streak.last_real_success_at),
        "next_run_due_at": _epoch(due_at),
        "allowed_age_seconds": int(pipeline.allowed_age_seconds),
        "last_read_at": _epoch(last_read_at),
        "last_read_ok": 1 if last_read_ok else 0,
        "read_at": _epoch(now),
        "runbook": pipeline.runbook,
    }
