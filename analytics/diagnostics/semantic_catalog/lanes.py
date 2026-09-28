"""Decide which review group a change needs, from which YAML keys moved.

CODEOWNERS matches on file path, and a `sem_*.yml` holds the rule, the
implementation, the build and display prose in one file. So both groups were
auto-requested on every edit, including a typo fix, and being asked to review
something you have no say over is how people stop reading the requests.

The lane is decided by content:

  business  what the number MEANS moved — `business_rule`, or the metric was
            added, removed or retired.
  data      how it is COMPUTED moved — `anchored_on`, `filter`, `measure`,
            `type`, `source`.
  neither   prose, label, owner, detail_doc. Mechanical and display edits ask
            nobody. Business is told about build changes through the merge
            summary, not asked.

A change can be in both lanes at once; that is a real thing, not an error.
Deliberately keyed on the same field tuples the two seals use, so a field can
never be sealed by one group and routed to the other.
"""

from __future__ import annotations

from semantic_catalog.ratifications import BUILD_FIELDS, RULE_FIELDS
from semantic_catalog.records import MetricRecord, by_basename

BUSINESS = "business"
DATA = "data"

# Retiring a metric ends the thing the business group ruled on, so it is their
# call. `era: historical` on one leg retires one event, which is instrumentation
# and lands in the data lane with the rest of `anchored_on`.
_BUSINESS_FIELDS = (*RULE_FIELDS, "retired")


def _moved(old: MetricRecord, new: MetricRecord, fields: tuple[str, ...]) -> bool:
    return any(getattr(old, f) != getattr(new, f) for f in fields)


def classify(before: list[MetricRecord], after: list[MetricRecord]) -> dict[str, list[str]]:
    """Metric names per lane, plus the ones that need no review at all.

    A metric appears under `unreviewed` only when it changed and neither lane
    claimed it, so an empty result everywhere means nothing governed moved.
    """
    # Normalised on the file's basename, the same way the Slack diff is: the
    # before-set is parsed from a base worktree, so the absolute path differs on
    # EVERY record and whole-record equality would report the whole catalog as
    # changed — here, as every metric needing review.
    old_by_name, new_by_name = by_basename(before), by_basename(after)
    lanes: dict[str, list[str]] = {BUSINESS: [], DATA: [], "unreviewed": []}

    for name in sorted(set(old_by_name) | set(new_by_name)):
        old, new = old_by_name.get(name), new_by_name.get(name)
        if old is None or new is None:
            # A metric appearing or disappearing is a change to what the company
            # measures AND a new or removed build, so it is both lanes. There is
            # no prior seal to compare either half against.
            lanes[BUSINESS].append(name)
            lanes[DATA].append(name)
            continue
        business = _moved(old, new, _BUSINESS_FIELDS)
        data = _moved(old, new, BUILD_FIELDS)
        if business:
            lanes[BUSINESS].append(name)
        if data:
            lanes[DATA].append(name)
        if not business and not data and old != new:
            lanes["unreviewed"].append(name)
    return lanes


def teams(lanes: dict[str, list[str]]) -> list[str]:
    """GitHub team slugs to request, in a stable order."""
    return [f"semantic-layer-{lane}" for lane in (DATA, BUSINESS) if lanes.get(lane)]


def summary(lanes: dict[str, list[str]]) -> str:
    """One line for a CI log or a PR comment."""
    parts = []
    for lane in (BUSINESS, DATA):
        if lanes.get(lane):
            parts.append(f"{lane}: {', '.join(lanes[lane])}")
    if lanes.get("unreviewed"):
        parts.append(f"no review needed: {', '.join(lanes['unreviewed'])}")
    return " · ".join(parts) if parts else "no governed change"
