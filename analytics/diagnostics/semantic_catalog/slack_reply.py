"""Post threaded Slack replies linking the Sigma build tasks a run created.

Stdlib only. The HTTP call goes through an injectable ``urlopen`` seam so tests
never touch the network. The bot token is passed in by the caller (read from the
SLACK_APP_BOT_TOKEN env var in cli.py, never from the command line); nothing here
reads the environment.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable, Iterable, Mapping
from typing import Any

SLACK_POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"


def reply_in_thread(
    token: str,
    channel: str,
    thread_ts: str,
    tasks: Iterable[Mapping[str, Any]],
    *,
    urlopen: Callable[..., Any] = urllib.request.urlopen,
) -> None:
    """Post one threaded reply per created task, each linking its ClickUp task.

    tasks are the dicts emitted by ``cli --emit-created`` (metric, task_id, url).
    Raises RuntimeError on a Slack ok:false body (200 with an error), matching the
    other clients in this package.
    """
    for t in tasks:
        text = f"Sigma build task created for `{t['metric']}`: {t['url']}"
        body = json.dumps({"channel": channel, "thread_ts": thread_ts, "text": text}).encode()
        req = urllib.request.Request(SLACK_POST_MESSAGE_URL, data=body, method="POST")
        req.add_header("Authorization", f"Bearer {token}")
        req.add_header("Content-type", "application/json; charset=utf-8")
        with urlopen(req, timeout=30) as resp:  # fixed Slack URL, not user input
            d = json.load(resp)
        # chat.postMessage returns HTTP 200 with ok:false on errors; check the body.
        if not d.get("ok"):
            raise RuntimeError(f"Slack thread reply failed for {t['metric']}: {d.get('error', 'unknown')}")
