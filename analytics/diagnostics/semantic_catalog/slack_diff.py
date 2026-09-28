"""Compose the Slack change summary for a governed-definition merge.

Diffs the before/after record sets and states, loudly, whether every review
group the change NEEDED approved. The soft gate relies on this: a merge missing
a required approval is announced as incomplete, never hidden.

Which groups it needed comes from the lane classifier, not from the file path.
This is also where the business group hears about a build change: told, not
asked.
"""

from __future__ import annotations

from semantic_catalog.records import MetricRecord
from semantic_catalog.records import by_basename as _by_name


def changed_metric_names(before: list[MetricRecord], after: list[MetricRecord]) -> list[str]:
    """Names added, removed, or changed in any field; feeds the thread anchor message."""
    a, b = _by_name(before), _by_name(after)
    names = set(a.keys() ^ b.keys())
    names.update(n for n in a.keys() & b.keys() if a[n] != b[n])
    return sorted(names)


def _half_line(name: str, half: str, old_date, new_date, old_stale, new_stale) -> list[str]:
    lines = []
    if old_date != new_date:
        lines.append(f"• {half} sign-off: {name} — {old_date or 'pending'} → {new_date or 'pending'}")
    elif old_stale != new_stale:
        # Staleness can flip with no date moving: correcting a mis-pasted seal is
        # a sidecar-only edit. Without this branch the anchor counts the metric as
        # changed while the summary shows no bullet for it, which is exactly the
        # silence DATA-2249 exists to remove.
        state = "stale (the sealed content changed since it was signed)" if new_stale else "current"
        lines.append(f"• {half} sign-off: {name} — now {state}")
    return lines


def diff_records(before: list[MetricRecord], after: list[MetricRecord]) -> list[str]:
    # Definition TEXT is deliberately not rendered anywhere in the thread: the
    # definitions run long and buried the lifecycle signal. The thread reports
    # WHICH metric changed and how; the PR diff is where you read the wording.
    a, b = _by_name(before), _by_name(after)
    lines: list[str] = []
    # Said once, up top: a seal-scheme change re-stamps every fingerprint at
    # once, so without this the summary below reads as several metrics breaking
    # in one merge rather than as one bookkeeping change.
    if any(a[n].seal_scheme != b[n].seal_scheme for n in a.keys() & b.keys()):
        lines.append(
            "• seals recomputed under a new scheme: the sign-off lines below are "
            "recomputations, not new approvals"
        )
    for name in sorted(b.keys() - a.keys()):
        lines.append(f"• added: {name}")
    for name in sorted(a.keys() - b.keys()):
        lines.append(f"• removed: {name}")
    for name in sorted(a.keys() & b.keys()):
        old, new = a[name], b[name]
        if old.business_rule != new.business_rule:
            lines.append(f"• rule: {name} — what the number counts changed")
        if old.anchored_on != new.anchored_on:
            # The edit that used to move nothing. It decides which users are
            # counted, so it gets its own line rather than hiding inside "build".
            lines.append(f"• anchor: {name} — the events that feed it changed")
        if any(getattr(old, f) != getattr(new, f) for f in ("filter", "measure", "metric_type", "source")):
            lines.append(f"• build: {name} — how it is computed changed")
        if old.definition != new.definition:
            lines.append(f"• wording: {name} (description updated, no sign-off affected)")
        # A scheme change re-stamps both halves, so per-half lines would repeat
        # the header above once per metric per half.
        if old.seal_scheme == new.seal_scheme:
            lines.extend(
                _half_line(name, "rule", old.rule_approved, new.rule_approved, old.rule_stale, new.rule_stale)
            )
            lines.extend(
                _half_line(
                    name, "build", old.build_approved, new.build_approved, old.build_stale, new.build_stale
                )
            )
        if old.value_at_signing != new.value_at_signing and new.value_at_signing is not None:
            lines.append(
                f"• value at signing: {name} — {old.value_at_signing or 'none'} → {new.value_at_signing}"
            )
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
    required: list[str] | None = None,
) -> str:
    """`required` is the lanes this diff actually needed (see `lanes.classify`).

    A lane nobody was asked for must not be rendered as a missing approval, or
    routing changes nothing: the merge summary would go on warning about a
    business sign-off on a build-only change and teach everyone to ignore it.
    Omitted means both lanes, which is the pre-routing behavior.
    """
    body = ["*Semantic layer updated*", f"PR: {pr_url}", ""]
    changes = diff_records(before, after)
    body.extend(changes if changes else ["(no metric-level changes detected)"])
    body.append("")

    needed = set(required) if required is not None else {"data", "business"}
    marks = []
    missing = False
    for lane in ("data", "business"):
        if lane not in needed:
            marks.append(f"{lane} — (not required)")
            continue
        ok = bool(coverage.get(lane))
        missing = missing or not ok
        marks.append(f"{lane} {'✓' if ok else '✗'}")
    # One authoritative line; the live approval history is in the thread now.
    line = "review coverage: " + " · ".join(marks)
    if missing:
        line = f":warning: {line}"
    body.append(line)
    return "\n".join(body)
