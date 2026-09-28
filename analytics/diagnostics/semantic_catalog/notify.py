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
    tried_dm = bool(owner)
    if tried_dm and post(token, owner, text):
        return "degrade notice sent to the owner"
    # A DM can be refused for a reason a channel post would not hit, a missing
    # im:write being the obvious one. Reroute rather than drop it. Only claim a
    # refusal when one happened: with no owner configured, saying the DM was
    # refused sends a reader hunting a Slack permission problem that is really a
    # missing env var.
    if channel:
        note = "\n(DM to the owner was refused, so this went to the channel instead.)" if tried_dm else ""
        if post(token, channel, text + note):
            if tried_dm:
                return "owner DM refused; degrade notice sent to the channel instead"
            return f"no {OWNER_ENV} set; degrade notice sent to the channel"
    if tried_dm:
        return "Slack refused both the owner DM and the channel fallback; degrade notice not delivered"
    return f"no {OWNER_ENV} set and the channel post failed; degrade notice not delivered"
