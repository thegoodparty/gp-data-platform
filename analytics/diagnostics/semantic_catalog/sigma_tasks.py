"""Create interim ClickUp tasks for metrics that were just ratified (DATA-2199).

Pure detection + payload logic here; the ClickUp HTTP calls live in
semantic_catalog.clickup_client. A task fires when a metric transitions to a
new ratified date; re-run idempotency is handled downstream by a check against
ClickUp (see sync()).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from semantic_catalog import ratifications
from semantic_catalog.records import MetricRecord


def build_key(rec: MetricRecord) -> str:
    """Dedupe key: metric name plus the fingerprint of the definition to build.

    Keyed on the DEFINITION, not on the ratified date. What someone has to build
    in Sigma follows from what the metric means; correcting a date changes
    nothing about the work. Keying on the date minted a rival task every time a
    date moved, and DATA-2249's reconciliation produced three of them in two
    days, each landing on an assignee for work already finished. A genuine
    definition change still yields a new fingerprint, so it still yields a task.
    """
    return f"{rec.name}@{ratifications.definition_sha(rec)}"


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


def _effective_ratified(rec: MetricRecord) -> str | None:
    """The sign-off a build task may trust, or None if there is none to trust.

    A stale date certifies a definition that has since changed, so it counts as
    no sign-off at all (DATA-2249). Otherwise this task would tell someone to
    build a definition nobody approved, and the task text renders the date with
    no stale marker, so the catalog's warning would not travel with it.
    """
    return None if rec.ratified_stale else rec.ratified


def newly_ratified(before: list[MetricRecord], after: list[MetricRecord]) -> list[MetricRecord]:
    """Metrics whose trustworthy ratified date is set in `after` and differs from `before`.

    Excludes still-pending, stale and retired metrics. A brand-new metric that
    arrives already ratified counts as newly ratified. Because staleness is part
    of the comparison, correcting a fingerprint fires the task the stale entry
    never got, even though the date itself did not move.
    """
    prev = {r.name: r for r in before}
    out: list[MetricRecord] = []
    for rec in after:
        if rec.retired or _effective_ratified(rec) is None:
            continue
        old = prev.get(rec.name)
        if old is None or _effective_ratified(old) != _effective_ratified(rec):
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
