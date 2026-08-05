"""Parse/render the PR comment that persists the Slack governance-thread state.

One bot comment per governed PR: a human-visible link to the Slack thread, then
the state as JSON inside an HTML comment. Pure module: no network, no env. A
missing or corrupt marker parses to None, which callers treat as "bootstrap a
fresh thread" (degrade, never crash).
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field

MARKER_OPEN = "<!-- semantic-layer-thread v1"
MARKER_CLOSE = "-->"
_MARKER_RE = re.compile(re.escape(MARKER_OPEN) + r"\n(?P<json>.*?)\n" + re.escape(MARKER_CLOSE), re.DOTALL)


@dataclass
class ThreadState:
    ts: str
    channel: str
    permalink: str
    announced: dict[str, str | None] = field(default_factory=lambda: {"data": None, "business": None})
    pr_state: str = "open"  # "open" | "closed"
    merged: bool = False


def is_marker(body: str) -> bool:
    return MARKER_OPEN in body


def parse(body: str) -> ThreadState | None:
    m = _MARKER_RE.search(body)
    if m is None:
        return None
    try:
        d = json.loads(m.group("json"))
        return ThreadState(
            ts=str(d["ts"]),
            channel=str(d["channel"]),
            permalink=str(d.get("permalink", "")),
            announced={
                "data": d.get("announced", {}).get("data"),
                "business": d.get("announced", {}).get("business"),
            },
            pr_state=str(d.get("pr_state", "open")),
            merged=bool(d.get("merged", False)),
        )
    except (json.JSONDecodeError, KeyError, TypeError, AttributeError):
        return None  # corrupt marker degrades to "no marker" so the thread re-bootstraps


def render(state: ThreadState) -> str:
    payload = json.dumps(asdict(state), indent=1, sort_keys=True)
    return f"Slack governance thread: {state.permalink}\n{MARKER_OPEN}\n{payload}\n{MARKER_CLOSE}"
