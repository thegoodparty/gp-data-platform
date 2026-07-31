"""Create interim ClickUp tasks for metrics that were just ratified (DATA-2199).

Pure detection + payload logic here; the ClickUp HTTP calls live in
semantic_catalog.clickup_client. A task fires when a metric transitions to a
new ratified date; re-run idempotency is handled downstream by a check against
ClickUp (see sync()).
"""

from __future__ import annotations

from dataclasses import dataclass

from semantic_catalog.records import MetricRecord


def build_key(rec: MetricRecord) -> str:
    """Dedupe key: metric name plus the ratified date it was shipped under."""
    return f"{rec.name}@{rec.ratified}"


@dataclass(frozen=True)
class TaskPayload:
    name: str
    markdown_description: str
    build_key: str


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


def task_payload(rec: MetricRecord) -> TaskPayload:
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
