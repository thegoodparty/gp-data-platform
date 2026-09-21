"""Read `anchored_on` off governed metrics (DATA-2421).

The semantic layer is the kernel: a metric declares the raw events that feed it, and
both the dbt macro and omni's health monitor derive from that declaration rather than
holding their own copy. This module is the one parser for that shape.
"""

from __future__ import annotations

from typing import Any

META_KEY = "anchored_on"


def parse_anchors(doc: dict[str, Any]) -> dict[str, list[dict[str, str | None]]]:
    """Map metric name -> normalised leg list, for metrics that declare an anchor.

    A leg is ``{"event", "path", "era"}`` with absent keys normalised to None, so every
    consumer sees the same shape. ``era: historical`` marks a leg kept for continuity
    that is not expected to still fire.
    """
    anchors: dict[str, list[dict[str, str | None]]] = {}
    for metric in doc.get("metrics") or []:
        declared = ((metric.get("config") or {}).get("meta") or {}).get(META_KEY)
        if not declared:
            continue
        legs = []
        for leg in declared:
            event = leg.get("event")
            if not event:
                raise ValueError(f"{metric.get('name')}: every {META_KEY} leg needs an 'event' key")
            legs.append({"event": event, "path": leg.get("path"), "era": leg.get("era")})
        anchors[metric["name"]] = legs
    return anchors
