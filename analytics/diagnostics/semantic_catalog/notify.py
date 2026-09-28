"""Deliver the instrument-health degrade notice to the owner, not the channel.

When the cross-repo evidence read cannot run, every metric in the merge summary
is unverified against whether its declared events are still firing. Saying
nothing would rebuild the silent-green bug the evidence link exists to remove.
But #data-alignment is where the review groups read metric news, and a standing
line about a token they cannot provision is what teaches people to scroll past
the alerts they can act on. So it goes to the owner by DM.

On #1076's pattern: a refused DM falls back to the channel rather than vanishing.
If Slack refuses both, the caller only annotates — the weekly token-health
workflow already fails loudly on a dead Slack token, which is the only way both
posts can be refused, and a second alarm for it would just turn governed merges
red for a condition that is already covered.
"""

from __future__ import annotations

import http.client
import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable

TOKEN_ENV = "SLACK_APP_BOT_TOKEN"
OWNER_ENV = "SLACK_OWNER_DM"
CHANNEL_ENV = "SLACK_CHANNEL_ID"
_API = "https://slack.com/api/chat.postMessage"
_TIMEOUT = 20

Poster = Callable[[str, str, str], bool]


def render(problems: list[str]) -> str:
    lines = ["Semantic-layer publish could not check build approvals against instrument health:"]
    lines.extend(f"- {problem}" for problem in problems)
    lines.append("")
    lines.append(
        "Metrics merged in this run were NOT verified against whether their declared events "
        "are still firing. Clears once OMNI_READ_TOKEN (or ORG_READ_TOKEN) can read "
        "thegoodparty/omni."
    )
    return "\n".join(lines)


def _post(token: str, channel: str, text: str) -> bool:
    """True only when Slack says ok. chat.postMessage returns HTTP 200 with
    {"ok": false} on a bad channel or a missing scope, so the body is the answer."""
    request = urllib.request.Request(
        _API,
        data=json.dumps({"channel": channel, "text": text}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT) as response:
            return bool(json.loads(response.read().decode()).get("ok"))
    # HTTPException, not just OSError: a connection dropped mid-body raises
    # IncompleteRead, which descends from HTTPException and would otherwise
    # escape the never-raises contract and fail a publish over a blip. Same
    # reason evidence.py names it explicitly.
    except (urllib.error.URLError, TimeoutError, OSError, ValueError, http.client.HTTPException):
        return False


def notify(
    problems: list[str],
    *,
    token: str | None = None,
    owner: str | None = None,
    channel: str | None = None,
    post: Poster = _post,
) -> str:
    """Send the notice; return a one-line outcome for the job log. Never raises.

    Raising here would fail a publish that also opens the ratification PR, over a
    Slack hiccup that says nothing about whether the merge was sound.
    """
    if not problems:
        return "instrument health checked; nothing to report"
    token = token if token is not None else os.environ.get(TOKEN_ENV)
    owner = owner if owner is not None else os.environ.get(OWNER_ENV)
    channel = channel if channel is not None else os.environ.get(CHANNEL_ENV)
    text = render(problems)
    if not token:
        return f"no {TOKEN_ENV}; degrade notice not delivered"
    # Report only what was actually attempted. Every outcome here is read by a
    # person deciding what to fix, and each wrong guess sends them somewhere
    # different: a refused DM means a Slack permission, an unset owner means an
    # env var, an untried channel means neither. Claiming a refusal that never
    # happened is the same class of lie as a guard reporting green while it did
    # not run, which is the bug this whole module exists to remove.
    if owner:
        if post(token, owner, text):
            return "degrade notice sent to the owner"
        failed = "the owner DM was refused"
        # A DM can be refused for a reason a channel post would not hit, a
        # missing im:write being the obvious one. Reroute rather than drop it.
        note = "\n(DM to the owner was refused, so this went to the channel instead.)"
    else:
        failed = f"no {OWNER_ENV} is set"
        note = ""

    if not channel:
        return f"{failed} and no {CHANNEL_ENV} to fall back to; degrade notice not delivered"
    if post(token, channel, text + note):
        return f"{failed}; degrade notice sent to the channel"
    return f"{failed} and the channel post failed; degrade notice not delivered"
