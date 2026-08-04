import json

import pytest
from semantic_catalog import slack_reply


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def read(self):
        return json.dumps(self._payload).encode()


def _recorder(ok=True, error=None):
    calls = []

    def fake_urlopen(req, timeout=30):
        calls.append(req)
        return _FakeResp({"ok": True} if ok else {"ok": False, "error": error or "bad"})

    return fake_urlopen, calls


def test_reply_in_thread_posts_one_reply_per_task():
    urlopen, calls = _recorder(ok=True)
    tasks = [
        {"metric": "win_users", "task_id": "a", "url": "https://app.clickup.com/t/a"},
        {"metric": "active_serve_users", "task_id": "b", "url": "https://app.clickup.com/t/b"},
    ]
    slack_reply.reply_in_thread("tok", "C123", "1699.0001", tasks, urlopen=urlopen)
    assert len(calls) == 2
    first = json.loads(calls[0].data)
    assert first["channel"] == "C123"
    assert first["thread_ts"] == "1699.0001"
    assert "win_users" in first["text"] and "clickup.com/t/a" in first["text"]
    # Token travels in the header, never on the command line.
    assert calls[0].get_header("Authorization") == "Bearer tok"


def test_reply_in_thread_raises_on_slack_error():
    urlopen, _ = _recorder(ok=False, error="channel_not_found")
    tasks = [{"metric": "win_users", "task_id": "a", "url": "u"}]
    with pytest.raises(RuntimeError, match="channel_not_found"):
        slack_reply.reply_in_thread("tok", "C123", "ts", tasks, urlopen=urlopen)


def test_reply_in_thread_noop_on_empty_tasks():
    urlopen, calls = _recorder(ok=True)
    slack_reply.reply_in_thread("tok", "C123", "ts", [], urlopen=urlopen)
    assert calls == []
