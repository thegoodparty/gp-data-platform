"""Slack Web API helpers for the governance thread (post, permalink, Sigma replies).

Stdlib only. The HTTP call goes through an injectable ``urlopen`` seam so tests
never touch the network. The bot token is passed in by the caller (read from the
SLACK_APP_BOT_TOKEN env var in cli.py, never from the command line); nothing here
reads the environment.
"""

from __future__ import annotations

import json
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from typing import Any

SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
SLACK_GET_PERMALINK_URL = "https://slack.com/api/chat.getPermalink"


def _check(d: Mapping[str, Any], what: str) -> Mapping[str, Any]:
    # Slack returns HTTP 200 with ok:false on errors; check the body.
    if not d.get("ok"):
        raise RuntimeError(f"{what} failed: {d.get('error', 'unknown')}")
    return d


def post_message(
    token: str,
    channel: str,
    text: str,
    *,
    thread_ts: str | None = None,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
) -> str:
    """Post a message (threaded when thread_ts is given); return its ts."""
    payload: dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    req = urllib.request.Request(SLACK_POST_MESSAGE_URL, data=json.dumps(payload).encode(), method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Content-type", "application/json; charset=utf-8")
    with urlopen(req, timeout=30) as resp:  # fixed Slack URL, not user input
        d = json.load(resp)
    return str(_check(d, "Slack chat.postMessage")["ts"])


def get_permalink(
    token: str,
    channel: str,
    ts: str,
    *,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
) -> str:
    """Resolve the permanent URL of a posted message (for the PR marker comment)."""
    qs = urllib.parse.urlencode({"channel": channel, "message_ts": ts})
    req = urllib.request.Request(f"{SLACK_GET_PERMALINK_URL}?{qs}", method="GET")
    req.add_header("Authorization", f"Bearer {token}")
    with urlopen(req, timeout=30) as resp:
        d = json.load(resp)
    return str(_check(d, "Slack chat.getPermalink")["permalink"])


def reply_in_thread(
    token: str,
    channel: str,
    thread_ts: str,
    tasks: Iterable[Mapping[str, Any]],
    *,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
) -> None:
    """Post one threaded reply per created task, each linking its ClickUp task."""
    for t in tasks:
        text = f"Sigma build task created for `{t['metric']}`: {t['url']}"
        try:
            post_message(token, channel, text, thread_ts=thread_ts, urlopen=urlopen)
        except RuntimeError as e:
            raise RuntimeError(f"Slack thread reply failed for {t['metric']}: {e}") from e
