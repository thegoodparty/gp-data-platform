"""Create interim ClickUp tasks for metrics that were just ratified (DATA-2199).

Pure detection + payload logic here; the ClickUp HTTP calls live in
semantic_catalog.clickup_client. A task fires when a metric transitions to a
new ratified date; re-run idempotency is handled downstream by a check against
ClickUp (see sync()).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from semantic_catalog.records import MetricRecord


def build_key(rec: MetricRecord) -> str:
    """Dedupe key: metric name plus the ratified date it was shipped under."""
    return f"{rec.name}@{rec.ratified}"


def task_url(task_id: str) -> str:
    """Web link for a created ClickUp task (what we post back into the Slack thread)."""
    return f"https://app.clickup.com/t/{task_id}"


@dataclass(frozen=True)
class TaskPayload:
    name: str
    markdown_description: str
    build_key: str
    # ClickUp user ids to assign on creation. Empty tuple => leave unassigned.
    assignee_ids: tuple[int, ...] = ()


_DESCRIPTION = """\
Build this ratified semantic-layer metric in Sigma.

Metric: {name}
Definition: {definition}
Ratified: {ratified}

Created automatically when the metric was ratified and merged. This is an interim
step until the dbt to Sigma automation (DATA-2200) or Sigma's native OSI write-back
is in place. Check this off once the metric exists in Sigma, and link the Sigma
object here.

Build key: {build_key}
"""


def task_payload(rec: MetricRecord, assignee_ids: tuple[int, ...] = ()) -> TaskPayload:
    # label and definition are echoed verbatim from config; the no-em-dash/no-emoji copy
    # rule is enforced upstream at the governance layer (see DATA-2211).
    key = build_key(rec)
    return TaskPayload(
        name=f"Build in Sigma: {rec.label} ({rec.name})",
        markdown_description=_DESCRIPTION.format(
            name=rec.name,
            definition=rec.definition,
            ratified=rec.ratified,
            build_key=key,
        ),
        build_key=key,
        assignee_ids=assignee_ids,
    )


def newly_ratified(before: list[MetricRecord], after: list[MetricRecord]) -> list[MetricRecord]:
    """Metrics whose ratified date is set in `after` and differs from `before`.

    Excludes still-pending and retired metrics. A brand-new metric that arrives
    already ratified counts as newly ratified.
    """
    prev = {r.name: r for r in before}
    out: list[MetricRecord] = []
    for rec in after:
        if rec.retired or not rec.ratified:
            continue
        old = prev.get(rec.name)
        if old is None or old.ratified != rec.ratified:
            out.append(rec)
    return out


class ClickUpTaskClient(Protocol):
    def find_task_by_build_key(self, list_id: str, field_id: str, build_key: str) -> str | None: ...

    def create_task(self, list_id: str, payload: TaskPayload, field_id: str) -> str: ...


@dataclass(frozen=True)
class CreatedTask:
    """A ClickUp task this run created, with the link we post into the Slack thread."""

    metric_name: str
    task_id: str
    url: str


@dataclass(frozen=True)
class SyncResult:
    created: tuple[CreatedTask, ...]
    skipped: tuple[str, ...]


def sync(
    client: ClickUpTaskClient,
    list_id: str,
    field_id: str,
    before: list[MetricRecord],
    after: list[MetricRecord],
    assignee_ids: tuple[int, ...] = (),
) -> SyncResult:
    """Create one ClickUp task per newly-ratified metric, skipping any that
    already exist in ClickUp (keyed on the build key custom field). New tasks
    are assigned to assignee_ids."""
    created: list[CreatedTask] = []
    skipped: list[str] = []
    for rec in newly_ratified(before, after):
        key = build_key(rec)
        if client.find_task_by_build_key(list_id, field_id, key) is not None:
            skipped.append(rec.name)
            continue
        task_id = client.create_task(list_id, task_payload(rec, assignee_ids), field_id)
        created.append(CreatedTask(metric_name=rec.name, task_id=task_id, url=task_url(task_id)))
    return SyncResult(created=tuple(created), skipped=tuple(skipped))
