"""Collects one dbt Cloud pipeline's finished runs into records, and the run's
streak and due time.

The error contract is the point of this module: a collector never raises out of
its task. Any failure of the tool comes back as `read_ok=False` with no records,
so one tool's outage cannot take the reader down and the status update still goes
out with `last_read_ok = 0`.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from include.pipelines.models import Pipeline
from include.reader.dbt_cloud import ArtifactUnavailable, DbtCloudClient
from include.reader.models import RUN_ERROR, RUN_SUCCESS, Run, Step, Streak
from include.reader.records import (
    build_run_record,
    last_due_before,
    next_run_due,
    next_streak,
    streak_to_payload,
    watched_warning,
)
from include.reader.state import ReaderState

logger = logging.getLogger("airflow.task")

# Enough history to find the latest real success on a cold start. The nightly runs
# twice a day, so this covers a fortnight of them, and it is one paginated read.
COLD_START_LOOKBACK = timedelta(days=14)

# How far before the cursor a window reaches. A run finishing just before the
# cursor can still be picked up, so a restart mid-window leaves no gap.
WINDOW_OVERLAP = timedelta(hours=1)


@dataclass(frozen=True)
class CollectResult:
    records: list[dict[str, Any]] = field(default_factory=list)
    # None means "no saved streak and no successful read": the status update then
    # says last_read_ok = 0 and nothing claims a recovery.
    streak: Streak | None = None
    cursor: datetime | None = None
    due_at: datetime | None = None
    read_ok: bool = False
    watched_warning: dict[str, Any] | None = None


def pick_step(steps: list[Step], run: Run) -> int | None:
    """The step whose artifact says what happened.

    On a failed run that is the step that failed. On a green run it is the dbt
    invocation, because the last step of a multi-step job is often `docs generate`
    and its artifact says nothing about the build's tests.
    """
    if not steps:
        return None
    if run.outcome == "error":
        for step in steps:
            if step.status == RUN_ERROR:
                return step.index
    build_steps = [s for s in steps if "dbt build" in s.name.lower()]
    if build_steps:
        return build_steps[-1].index
    succeeded = [s for s in steps if s.status == RUN_SUCCESS]
    if succeeded:
        return succeeded[-1].index
    return steps[-1].index


def _artifact(client: DbtCloudClient, run: Run) -> dict[str, Any] | None:
    """The step artifact, or None. A missing artifact degrades the record to
    status-only rather than failing the run: the ticket still gets a link."""
    try:
        step = pick_step(client.list_steps(run.run_id), run)
        if step is None:
            return None
        return client.fetch_run_results(run.run_id, step)
    except ArtifactUnavailable as exc:
        logger.warning("reader: no artifact for run %s (%s)", run.run_id, exc.__class__.__name__)
        return None
    except Exception as exc:
        logger.warning("reader: artifact read failed for run %s (%s)", run.run_id, exc.__class__.__name__)
        return None


def collect(pipeline: Pipeline, client: DbtCloudClient, state: ReaderState, now: datetime) -> CollectResult:
    """Every finished run the reader has not yet seen, plus the new streak and due time."""
    tool = pipeline.tool
    saved = state.streaks.get(pipeline.pipeline)
    window = None if saved is None else state.window_start(tool, int(WINDOW_OVERLAP.total_seconds()))
    finished_after = now - COLD_START_LOOKBACK if window is None else window
    # With no read this run, the due time has to come from state. The last real
    # success is the only start-like time state holds, and anchoring there errs
    # later, which is the direction that does not page a run that was not missed.
    due_at_fallback = next_run_due(pipeline.schedule, saved.last_real_success_at if saved else None) or (
        last_due_before(pipeline.schedule, now)
    )

    job_id = pipeline.tool_ids.job_id
    if job_id is None:
        logger.warning("reader: pipeline %s has no job id", pipeline.pipeline)
        return CollectResult(streak=saved, due_at=due_at_fallback, read_ok=False)

    try:
        runs = client.list_runs(job_id, finished_after)
        unfinished = client.first_unfinished(job_id)
    except Exception as exc:
        logger.warning("reader: %s read failed (%s)", tool, exc.__class__.__name__)
        return CollectResult(streak=saved, due_at=due_at_fallback, read_ok=False)

    fresh = [r for r in runs if not state.already_seen(tool, r.run_id)]
    fresh.sort(key=lambda r: r.finished_at or now)

    records: list[dict[str, Any]] = []
    # Per fresh run, the watched warning its own artifact shows (None when it shows
    # none). Kept so the newest finished run's own answer can be the authority.
    found_by_run: dict[int, dict[str, Any] | None] = {}
    for run in fresh:
        artifact = _artifact(client, run)
        records.append(build_run_record(pipeline, run, artifact))
        # A watched warning only exists on a green run, which is why the artifact
        # is read on every finished run and not only on failures.
        found_by_run[run.run_id] = watched_warning(pipeline, artifact)

    # The newest finished run decides, and carries forward when it was already sent:
    # a cycle with no new runs must still report a warning the latest nightly showed,
    # or a recording rule would resolve an alert that is still true. A newer run that
    # shows none clears it, which is what stops a fixed check alerting forever.
    newest = max((r for r in runs if r.finished_at is not None), key=lambda r: r.finished_at, default=None)
    if newest is None or newest.run_id not in found_by_run:
        warning = state.warnings.get(pipeline.pipeline)
    else:
        warning = found_by_run[newest.run_id]

    # The streak folds the whole window, not only the new runs: on a cold start the
    # window is the history, and folding it again is idempotent because a run's
    # effect depends only on its own status.
    streak = next_streak(saved, pipeline, runs, read_ok=True)
    last_start = max(
        (r.started_at for r in runs if r.started_at),
        default=None,
    )
    if unfinished is not None and unfinished.started_at is not None:
        # A started-but-unfinished run counts as started, or a long nightly looks
        # like a missed run while it is still running.
        last_start = max(filter(None, [last_start, unfinished.started_at]), default=None)
    # With no observed start at all, the due time is the last cron occurrence: the
    # schedule says a run was due then and none is on record, so rule 2 fires.
    due_at = (
        next_run_due(pipeline.schedule, last_start)
        or due_at_fallback
        or last_due_before(pipeline.schedule, now)
    )
    newest_finish = max((r.finished_at for r in runs if r.finished_at), default=None)
    return CollectResult(
        records=records,
        streak=streak,
        cursor=newest_finish,
        due_at=due_at,
        read_ok=True,
        watched_warning=warning,
    )


def payload_for(result: CollectResult) -> dict[str, Any]:
    """The collector's result as XCom-safe JSON: datetimes become ISO strings."""
    return {
        "records": result.records,
        "streak": streak_to_payload(result.streak),
        "cursor": result.cursor.isoformat() if result.cursor else None,
        "due_at": result.due_at.isoformat() if result.due_at else None,
        "read_ok": result.read_ok,
        "watched_warning": result.watched_warning,
    }


def collect_tool(
    pipelines: list[Pipeline], client: DbtCloudClient, state: ReaderState, now: datetime
) -> dict[str, Any]:
    """Every dbt Cloud pipeline's records in one payload, which is one DAG task.

    A pipeline whose read failed sets read_ok = False for the tool, so the status
    update says last_read_ok = 0 for it and the cursor does not move.
    """
    per_pipeline: dict[str, Any] = {}
    read_ok = True
    for pipeline in pipelines:
        result = collect(pipeline, client, state, now)
        per_pipeline[pipeline.pipeline] = payload_for(result)
        read_ok = read_ok and result.read_ok
    return {"tool": "dbt_cloud", "read_ok": read_ok, "pipelines": per_pipeline}
