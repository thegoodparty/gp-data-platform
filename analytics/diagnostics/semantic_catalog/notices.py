"""Decide who hears about a merge: the channel, the owner, or nobody.

#data-alignment exists to say that a metric changed. A run of mechanics merges
(seals, sidecar schema, routing, the catalog generator) posted summaries shaped
exactly like metric news, and a channel that cries wolf is one where the next
real post gets scrolled past.

The target is decided from the lanes the diff moved, the same classification the
review routing uses, plus one explicit escape hatch:

  channel   a metric's rule or build moved and nobody claimed the merge as
            mechanics. This is the news the channel is for.
  owner     something moved, but only prose — or the author labelled the PR
            mechanics. Routed to the owner's DM, not dropped: a mechanics claim
            has to stay checkable by the person who would catch a false one.
  skip      nothing moved at all. There is nothing to tell anyone.

The label DOWNGRADES rather than silences on purpose. A marker that can delete a
metric announcement outright is a hole in the thing the governance channel
exists to do.
"""

from __future__ import annotations

import json
import os

CHANNEL = "channel"
OWNER = "owner"
SKIP = "skip"


def route(lanes: dict[str, list[str]] | None, mechanics_label: bool = False) -> dict[str, str]:
    """Pick the target for a merge summary, with the reason it was picked.

    `lanes` is `--classify-lanes` output, or None when the step did not run. No
    classification means no grounds to suppress, so that case posts to the
    channel — failing toward the channel is the safe direction.
    """
    if lanes is None:
        return {"target": CHANNEL, "reason": "no base tree to classify against"}
    # `--classify-lanes` emits EMPTY lane lists in its no-base branch as well as
    # when a diff genuinely moved nothing, and only the real classification
    # carries `summary` (cli.py `_classify_lanes`). Without this the two cases
    # are indistinguishable and an unclassifiable merge would be read as
    # "nothing moved" and silently suppressed.
    if "summary" not in lanes:
        return {"target": CHANNEL, "reason": "no base tree to classify against"}

    governed = bool(lanes.get("business")) or bool(lanes.get("data"))
    if governed:
        if mechanics_label:
            return {"target": OWNER, "reason": "labelled a mechanics change"}
        return {"target": CHANNEL, "reason": "a metric's rule or build moved"}

    if lanes.get("unreviewed"):
        # Display prose on a real metric, which asks nobody for review but is
        # not dev work either. The owner hears it; the company does not need a
        # channel post about a description fix.
        return {"target": OWNER, "reason": "only display prose moved"}

    return {"target": SKIP, "reason": "no metric's rule or build moved"}


def main(argv: list[str] | None = None) -> int:
    """Read LANES (classify JSON) and MECHANICS_LABEL from env, print the route."""
    del argv  # env-var driven, matching semantic_catalog.composition
    raw = os.environ.get("LANES") or ""
    lanes = json.loads(raw) if raw.strip() else None
    label = os.environ.get("MECHANICS_LABEL", "").lower() == "true"
    print(json.dumps(route(lanes, label)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
