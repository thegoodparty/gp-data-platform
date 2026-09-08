"""Orchestrates one run of one flow against one destination.

Reads the flow's desired-state rows and this flow's latest logged payloads, runs the
guards, hands the buffered diff to the destination, and gives the destination a
callback that appends a batch's confirmed rows to sent_log as soon as that batch is
confirmed -- never end-of-run, so a later batch's failure cannot strand an earlier
batch's confirmations unlogged.
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


@dataclass(frozen=True)
class RunSummary:
    flow_id: str
    source_count: int
    to_send_count: int
    sent_count: int
    error_count: int
    orphaned_key_count: int
    errors: list[RowError] = field(default_factory=list)

    def as_line(self) -> str:
        """One deterministic line: what a wrapping DAG task should carry into its failure alert."""
        return (
            f"retl flow={self.flow_id} source={self.source_count} to_send={self.to_send_count} "
            f"sent={self.sent_count} errors={self.error_count} orphaned_keys={self.orphaned_key_count}"
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
        raw_key = row[flow.key_column]
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


def execute_run(
    *,
    connection: Any,
    flow: FlowConfig,
    log_table: str,
    destination: Destination,
) -> RunSummary:
    desired = read_source_payloads(connection, flow)
    if not desired:
        raise EmptySourceError(flow.flow_id)

    latest_sent = sent_log.read_latest_sent(connection, log_table=log_table, flow_id=flow.flow_id)
    to_send = compute_to_send(desired, latest_sent)

    # Buffered before any POST: a guard failure here must mean zero sends, not a
    # partial run discovered after batches already went out.
    buffered = list(to_send.items())
    if len(buffered) > flow.cap:
        raise SendCapExceededError(flow.flow_id, cap=flow.cap, actual=len(buffered))

    orphaned = orphaned_keys(latest_sent, desired)

    def _on_batch_confirmed(confirmed: dict[str, str]) -> None:
        sent_log.append_sent_log(connection, log_table=log_table, flow_id=flow.flow_id, confirmed=confirmed)

    delivery = destination.deliver(flow.flow_id, buffered, on_batch_confirmed=_on_batch_confirmed)

    return RunSummary(
        flow_id=flow.flow_id,
        source_count=len(desired),
        to_send_count=len(buffered),
        sent_count=len(delivery.confirmed),
        error_count=len(delivery.errors),
        orphaned_key_count=len(orphaned),
        errors=delivery.errors,
    )
