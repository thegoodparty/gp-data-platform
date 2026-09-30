"""Orchestrates one run of one flow against one destination.

`plan_run` reads the flow's desired-state rows and latest logged payloads, runs the
guards, and computes the diff; a dry run stops there. `execute_run` then hands the
diff to the destination, with each confirmed batch going straight to a SentLogWriter.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from . import databricks_io, sent_log
from .config import FlowConfig
from .destinations import Destination, RowError
from .diff import compute_to_send, orphaned_keys
from .payload import build_payload, serialize_payload

# A systemically-rejected convergence day must not dump ~74k lines into the
# process's own output; the histogram already carries the full-population shape.
INDIVIDUAL_ERROR_LINES_CAP = 20


class EmptySourceError(RuntimeError):
    """A broken upstream join must not read as a quiet day."""

    def __init__(self, flow_id: str):
        super().__init__(f"flow {flow_id!r}: source model returned zero rows")


class InvalidTrackingKeyError(RuntimeError):
    """A corrupt key must never be silently sent or logged: `str(None)` == 'None' would
    otherwise become a real HubSpot idProperty value, sent and logged so the row never
    retries, and a duplicate key would silently last-write-wins instead of failing loud.
    Raised while building payloads, before any guard or POST -- zero sends either way.
    """

    def __init__(self, flow_id: str, key_column: str, violation: str):
        self.flow_id = flow_id
        self.key_column = key_column
        self.violation = violation
        super().__init__(f"flow {flow_id!r}: key column {key_column!r} {violation}")


class SendCapExceededError(RuntimeError):
    """Zero rows are sent when the diff exceeds the flow's cap."""

    def __init__(self, flow_id: str, *, cap: int, actual: int):
        self.cap = cap
        self.actual = actual
        super().__init__(f"flow {flow_id!r}: {actual} rows to send exceeds cap {cap}; zero rows sent")


class EmptyLogError(RuntimeError):
    """An amnesia day -- a lost/recreated/truncated table, or a mis-pointed
    log_table config reading zero rows for this flow -- would otherwise look
    exactly like day one and silently re-send the full population. The volume cap
    cannot catch it: a legitimate recompute day also rewrites ~70% of rows, so
    "most of the population moved" is not itself a signal of a problem. This guard
    covers all three amnesia shapes with zero false alarms on any day the log
    genuinely has rows for this flow. Zero sends on failure, as with the other
    guards; applies to every destination, and to a dry run.
    """

    def __init__(self, flow_id: str, log_table: str):
        self.flow_id = flow_id
        self.log_table = log_table
        super().__init__(
            f"flow {flow_id!r}: {log_table} has no logged rows for this flow; "
            "pass --accept-empty-log only for a deliberate first run or post-reset run"
        )


@dataclass(frozen=True)
class RunSummary:
    flow_id: str
    source_count: int
    to_send_count: int
    sent_count: int
    error_count: int
    orphaned_key_count: int
    errors: list[RowError] = field(default_factory=list)
    dry_run: bool = False

    def as_line(self) -> str:
        """One deterministic line: what a wrapping DAG task should carry into its failure alert."""
        return (
            f"retl flow={self.flow_id} source={self.source_count} to_send={self.to_send_count} "
            f"sent={self.sent_count} errors={self.error_count} orphaned_keys={self.orphaned_key_count}"
            + (" dry_run=true" if self.dry_run else "")
        )


def error_report_lines(errors: Sequence[RowError]) -> list[str]:
    """Rejected-row detail for the process's own output: a histogram, then a capped sample.

    A wrapping DAG task can only carry into its failure alert what this process
    actually printed, so this is the only place row-level error codes reach
    process output. Nothing beyond the sanctioned error tuple (flow, tracking_key,
    error_code, property, retryable) -- never contact values or payload fragments.
    Empty input means no lines: a clean run must print nothing extra here.
    """
    if not errors:
        return []

    counts: dict[tuple[str, str | None, bool], int] = {}
    for error in errors:
        key = (error.error_code, error.property, error.retryable)
        counts[key] = counts.get(key, 0) + 1

    histogram_lines = [
        f"retl error_histogram code={code} property={prop} retryable={retryable} count={count}"
        for (code, prop, retryable), count in sorted(
            counts.items(), key=lambda item: (item[0][0], item[0][1] or "", item[0][2])
        )
    ]
    individual_lines = [
        f"retl error flow={error.flow_id} tracking_key={error.tracking_key} "
        f"code={error.error_code} property={error.property} retryable={error.retryable}"
        for error in errors[:INDIVIDUAL_ERROR_LINES_CAP]
    ]
    return histogram_lines + individual_lines


def read_source_payloads(connection: Any, flow: FlowConfig) -> dict[str, str]:
    """Fetch the flow's desired-state rows and serialize each one, keyed by tracking key."""
    with connection.cursor() as cursor:
        # flow.source_relation is trusted config, never a hardcoded string here (this
        # package is built before the model it reads exists), and not user input;
        # DB-API parameters cannot bind a relation name either way.
        cursor.execute(f"select * from {flow.source_relation}")
        rows = databricks_io.fetch_all_rows(cursor)

    payloads: dict[str, str] = {}
    for row in rows:
        try:
            raw_key = row[flow.key_column]
        except KeyError as exc:
            # A bare KeyError names only the column; the operator needs the flow too.
            raise InvalidTrackingKeyError(
                flow.flow_id, flow.key_column, "is not a column in the source model"
            ) from exc
        if raw_key is None:
            raise InvalidTrackingKeyError(flow.flow_id, flow.key_column, "is null")
        tracking_key = str(raw_key)
        if not tracking_key.strip():
            raise InvalidTrackingKeyError(flow.flow_id, flow.key_column, f"is blank ({raw_key!r})")
        if tracking_key in payloads:
            raise InvalidTrackingKeyError(
                flow.flow_id, flow.key_column, f"has a duplicate value {tracking_key!r}"
            )
        payload = build_payload(row, excluded_columns=flow.excluded_columns)
        payloads[tracking_key] = serialize_payload(payload)
    return payloads


@dataclass(frozen=True)
class RunPlan:
    """What a run would send, after every guard has passed."""

    flow_id: str
    source_count: int
    to_send: list[tuple[str, str]]
    orphaned_key_count: int

    def dry_run_summary(self) -> RunSummary:
        return RunSummary(
            flow_id=self.flow_id,
            source_count=self.source_count,
            to_send_count=len(self.to_send),
            sent_count=0,
            error_count=0,
            orphaned_key_count=self.orphaned_key_count,
            dry_run=True,
        )


def plan_run(*, connection: Any, flow: FlowConfig, accept_empty_log: bool = False) -> RunPlan:
    desired = read_source_payloads(connection, flow)
    if not desired:
        raise EmptySourceError(flow.flow_id)

    # Verified before a single row of the log is trusted: a table stamped for a
    # different flow would otherwise read as this flow's (non-empty) latest_sent,
    # which is exactly the shape the empty-log guard below cannot see through.
    sent_log.verify_log_table_identity(connection, flow.log_table, flow.flow_id)

    latest_sent = sent_log.read_latest_sent(connection, log_table=flow.log_table)
    if not latest_sent and not accept_empty_log:
        raise EmptyLogError(flow.flow_id, flow.log_table)

    # Buffered before any POST: a guard failure here must mean zero sends, not a
    # partial run discovered after batches already went out.
    to_send = list(compute_to_send(desired, latest_sent).items())
    if len(to_send) > flow.cap:
        raise SendCapExceededError(flow.flow_id, cap=flow.cap, actual=len(to_send))

    return RunPlan(
        flow_id=flow.flow_id,
        source_count=len(desired),
        to_send=to_send,
        orphaned_key_count=len(orphaned_keys(latest_sent, desired)),
    )


def execute_run(
    *,
    connection: Any,
    flow: FlowConfig,
    destination: Destination,
    accept_empty_log: bool = False,
) -> RunSummary:
    plan = plan_run(connection=connection, flow=flow, accept_empty_log=accept_empty_log)
    with sent_log.SentLogWriter(connection, flow.log_table) as writer:
        delivery = destination.deliver(flow.flow_id, plan.to_send, on_batch_confirmed=writer.add)

    return RunSummary(
        flow_id=flow.flow_id,
        source_count=plan.source_count,
        to_send_count=len(plan.to_send),
        sent_count=len(delivery.confirmed),
        error_count=len(delivery.errors),
        orphaned_key_count=plan.orphaned_key_count,
        errors=delivery.errors,
    )
