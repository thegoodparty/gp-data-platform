"""Canonical renderings of the two sealed layers of a metric definition.

A seal has to be computed from text, and the text has to be stable: two YAML
authorings that mean the same thing must render identically, or a reorder reads
as a new definition and expires an approval nobody changed.

Both renderings live here rather than in the parser because the dbt macro side
reads the same declarations, and a second derivation of either one would be a
second thing to keep in step.
"""

from __future__ import annotations

from typing import Any

# The only keys a business rule may carry. The rule is what we MEAN, and the
# test for whether a line belongs is that it names no event, no surface and no
# table. An open mapping would let the implementation layer drift back in one
# key at a time, which is the collapse this split exists to prevent.
RULE_KEYS = ("counts", "excludes", "known_gaps")


def _clean(text: Any) -> str:
    """Collapse a folded/blocked YAML scalar to one line, so re-wrapping a block
    scalar leaves the seal unchanged."""
    return " ".join(str(text or "").split())


def render_business_rule(declared: Any, metric: str) -> str | None:
    """Canonical one-line rendering of `config.meta.business_rule`, or None.

    Raises on a shape that cannot be reviewed as a rule: a missing `counts`, an
    unknown key, or a list item that is not text.
    """
    # `is None`, not falsy: `business_rule: {}` is a block someone started and
    # left empty, and reading it as "no rule at all" would skip business-group
    # routing and the rule half with no feedback to the author. It falls through
    # to the `counts` check below, which raises the right error.
    if declared is None:
        return None
    if not isinstance(declared, dict):
        raise ValueError(f"{metric}: business_rule must be a mapping with a 'counts' key")
    unknown = sorted(set(declared) - set(RULE_KEYS))
    if unknown:
        raise ValueError(
            f"{metric}: business_rule has unknown key(s) {', '.join(unknown)}. "
            f"Allowed: {', '.join(RULE_KEYS)}."
        )
    counts = _clean(declared.get("counts"))
    if not counts:
        raise ValueError(f"{metric}: business_rule needs a 'counts' line saying what the number counts")
    parts = [f"counts: {counts}"]
    for key in ("excludes", "known_gaps"):
        raw = declared.get(key) or []
        if isinstance(raw, str) or not isinstance(raw, list | tuple):
            raise ValueError(f"{metric}: business_rule.{key} must be a list of lines")
        items = [_clean(item) for item in raw]
        if any(not item for item in items):
            raise ValueError(f"{metric}: business_rule.{key} has an empty line")
        parts.append(f"{key}: {' ~ '.join(items)}")
    return " | ".join(parts)


def render_anchored_on(declared: Any, metric: str) -> str | None:
    """Canonical one-line rendering of `config.meta.anchored_on`, or None.

    Every qualifier is carried, including `era`. A historical leg is not
    decoration: removing one changes which rows the compiled predicate matches
    for past dates, so it belongs inside the build seal like any other leg.
    """
    # Same reasoning as the rule above: `anchored_on: []` is a started-and-left
    # declaration, not an absent one. Left falsy it would render as "" and seal
    # identically to no anchor at all, so a metric could declare an instrument,
    # name none, and read as though it had never claimed one.
    if declared is None:
        return None
    if not isinstance(declared, list | tuple):
        raise ValueError(f"{metric}: anchored_on must be a list of legs")
    if not declared:
        raise ValueError(f"{metric}: anchored_on is empty. Remove the key, or name a leg.")
    legs = []
    for leg in declared:
        if not isinstance(leg, dict) or not leg.get("event"):
            raise ValueError(f"{metric}: every anchored_on leg needs an 'event' key")
        parts = [f"event={leg['event']}"]
        if leg.get("path"):
            parts.append(f"path={leg['path']}")
        if leg.get("era"):
            parts.append(f"era={leg['era']}")
        excluding = leg.get("excluding") or {}
        if excluding:
            if not isinstance(excluding, dict):
                raise ValueError(f"{metric}: anchored_on excluding must be a mapping")
            rendered = []
            for prop, value in sorted(excluding.items()):
                values = value if isinstance(value, list | tuple) else [value]
                # Values sorted as well as property names: the excluded values
                # are a set, and a YAML reorder meaning the same thing must not
                # move the build seal and expire an approval nobody changed.
                rendered.append(f"{prop}={','.join(sorted(str(v) for v in values))}")
            parts.append(f"excluding={' '.join(rendered)}")
        legs.append(" ".join(parts))
    return " ; ".join(legs)


def rule_names_an_implementation(rule: str | None, anchored_on_declared: Any) -> list[str]:
    """Event names the rule text mentions, which are the implementation layer.

    The business group rules on what the number means, and cannot be asked to
    rule on which of three moments sharing an event name is the send. A rule
    that names an event has swallowed that question, so the two layers can no
    longer go stale independently and the routing split stops meaning anything.
    """
    if not rule or not anchored_on_declared:
        return []
    events = {str(leg["event"]) for leg in anchored_on_declared if isinstance(leg, dict) and leg.get("event")}
    return sorted(event for event in events if event in rule)
