"""Compose the Slack change summary for a governed-definition merge.

Diffs the before/after record sets and states, loudly, whether each review
group approved. The soft gate relies on this: a merge without both approvals is
announced as incomplete, never hidden.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from semantic_catalog.records import MetricRecord


def _by_name(records: list[MetricRecord]) -> dict[str, MetricRecord]:
    # Compare on the file's basename, not the absolute path the parser stores.
    # The before-set is parsed from a temp worktree, so the full path differs
    # for EVERY record and whole-record equality would report the entire
    # catalog as changed. The basename still catches a real change: a metric
    # moving between sem files.
    return {r.name: replace(r, yaml_file=Path(r.yaml_file).name) for r in records}


def changed_metric_names(before: list[MetricRecord], after: list[MetricRecord]) -> list[str]:
    """Names added, removed, or changed in any field; feeds the thread anchor message."""
    a, b = _by_name(before), _by_name(after)
    names = set(a.keys() ^ b.keys())
    names.update(n for n in a.keys() & b.keys() if a[n] != b[n])
    return sorted(names)


def diff_records(before: list[MetricRecord], after: list[MetricRecord]) -> list[str]:
    # Definition TEXT is deliberately not rendered anywhere in the thread: the
    # definitions run long and buried the lifecycle signal. The thread reports
    # WHICH metric changed and how; the PR diff is where you read the wording.
    a, b = _by_name(before), _by_name(after)
    lines: list[str] = []
    for name in sorted(b.keys() - a.keys()):
        lines.append(f"• added: {name}")
    for name in sorted(a.keys() - b.keys()):
        lines.append(f"• removed: {name}")
    for name in sorted(a.keys() & b.keys()):
        old, new = a[name], b[name]
        if old.definition != new.definition:
            lines.append(f"• changed: {name} (definition updated)")
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

    def _mark(ok: bool) -> str:
        return "✓" if ok else "✗"

    # One authoritative line; the live approval history is in the thread now.
    line = f"review coverage: data {_mark(bool(coverage.get('data')))} · business {_mark(bool(coverage.get('business')))}"
    if not (coverage.get("data") and coverage.get("business")):
        line = f":warning: {line}"
    body.append(line)
    return "\n".join(body)
