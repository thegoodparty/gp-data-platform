"""Read `anchored_on` off governed metrics (DATA-2421).

The semantic layer is the kernel: a metric declares the raw events that feed it, and
both the dbt macro and omni's health monitor derive from that declaration rather than
holding their own copy. This module is the one parser for that shape.
"""

from __future__ import annotations

from typing import Any

META_KEY = "anchored_on"


def parse_anchors(doc: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Map metric name -> normalised leg list, for metrics that declare an anchor.

    A leg is ``{"event", "path", "era", "excluding", "paywalled"}`` with absent keys
    normalised to None (``excluding`` to ``{}``, ``paywalled`` to False), so every
    consumer sees the same shape.

    ``path`` narrows a leg to one page-path slice of a site-wide event. ``excluding``
    narrows a leg by event property, as ``{property: value}``: one event name can cover
    several moments, and an exclusion is how a declaration keeps the ones the metric
    means. ``era: historical`` marks a leg kept for continuity that is not expected to
    still fire. ``paywalled`` marks a leg only a paying user can reach, which a metric
    used as a model label must drop or it learns who paid rather than who engaged.

    Every key is carried even where a given consumer ignores it. A parser that drops a
    qualifier reports a leg as wider than the metric actually counts.
    """
    anchors: dict[str, list[dict[str, Any]]] = {}
    for metric in doc.get("metrics") or []:
        declared = ((metric.get("config") or {}).get("meta") or {}).get(META_KEY)
        if not declared:
            continue
        legs = []
        for leg in declared:
            event = leg.get("event")
            if not event:
                raise ValueError(f"{metric.get('name')}: every {META_KEY} leg needs an 'event' key")
            legs.append(
                {
                    "event": event,
                    "path": leg.get("path"),
                    "era": leg.get("era"),
                    "excluding": leg.get("excluding") or {},
                    "paywalled": bool(leg.get("paywalled")),
                }
            )
        anchors[metric["name"]] = legs
    return anchors
