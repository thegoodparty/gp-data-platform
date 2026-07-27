"""Compose the Slack change summary for a governed-definition merge.

Diffs the before/after record sets and states, loudly, whether each review
group approved. The soft gate relies on this: a merge without both approvals is
announced as incomplete, never hidden.
"""

from __future__ import annotations

from semantic_catalog.records import MetricRecord


def _by_name(records: list[MetricRecord]) -> dict[str, MetricRecord]:
    return {r.name: r for r in records}


def diff_records(before: list[MetricRecord], after: list[MetricRecord]) -> list[str]:
    a, b = _by_name(before), _by_name(after)
    lines: list[str] = []
    for name in sorted(b.keys() - a.keys()):
        lines.append(f"• added: {name} — {b[name].definition}")
    for name in sorted(a.keys() - b.keys()):
        lines.append(f"• removed: {name} — {a[name].definition}")
    for name in sorted(a.keys() & b.keys()):
        old, new = a[name], b[name]
        if old.definition != new.definition:
            lines.append(f"• changed: {name}\n    before: {old.definition}\n    after:  {new.definition}")
        if old.ratified != new.ratified:
            lines.append(f"• ratified: {name} — {old.ratified or 'pending'} → {new.ratified or 'pending'}")
        if old.retired != new.retired:
            lines.append(f"• retired: {name} — {old.retired or 'active'} → {new.retired or 'active'}")
        if old.owner != new.owner:
            lines.append(f"• owner: {name} — {old.owner or '(none)'} → {new.owner or '(none)'}")
    return lines


def render_message(
    before: list[MetricRecord],
    after: list[MetricRecord],
    pr_url: str,
    coverage: dict,
) -> str:
    body = ["*Semantic layer updated*", f"PR: {pr_url}", ""]
    changes = diff_records(before, after)
    body.extend(changes if changes else ["(no metric-level changes detected)"])
    body.append("")
    missing = [g for g in ("data", "business") if not coverage.get(g)]
    if missing:
        noun = "group" if len(missing) == 1 else "groups"
        body.append(f":warning: review incomplete — missing approval from: {', '.join(missing)} {noun}")
    else:
        body.append(":white_check_mark: review complete — both groups approved")
    return "\n".join(body)
