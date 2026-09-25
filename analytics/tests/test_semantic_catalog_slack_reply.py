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


def _recorder(ok=True, error=None, extra=None):
    calls = []

    def fake_urlopen(req, timeout=30):
        calls.append(req)
        payload = {"ok": True, **(extra or {})} if ok else {"ok": False, "error": error or "bad"}
        return _FakeResp(payload)

    return fake_urlopen, calls


def test_post_message_returns_ts_and_posts_top_level():
    urlopen, calls = _recorder(ok=True, extra={"ts": "1722.0042"})
    ts = slack_reply.post_message("tok", "C123", "hello", urlopen=urlopen)
    assert ts == "1722.0042"
    body = json.loads(calls[0].data)
    assert body == {"channel": "C123", "text": "hello"}
    assert calls[0].get_header("Authorization") == "Bearer tok"


def test_post_message_threads_when_thread_ts_given():
    urlopen, calls = _recorder(ok=True, extra={"ts": "1722.0043"})
    slack_reply.post_message("tok", "C123", "reply", thread_ts="1722.0001", urlopen=urlopen)
    assert json.loads(calls[0].data)["thread_ts"] == "1722.0001"


def test_post_message_raises_on_slack_error():
    urlopen, _ = _recorder(ok=False, error="invalid_auth")
    with pytest.raises(RuntimeError, match="invalid_auth"):
        slack_reply.post_message("tok", "C123", "x", urlopen=urlopen)


def test_get_permalink_uses_get_with_params():
    urlopen, calls = _recorder(ok=True, extra={"permalink": "https://gp.slack.com/archives/C123/p1722"})
    link = slack_reply.get_permalink("tok", "C123", "1722.0042", urlopen=urlopen)
    assert link == "https://gp.slack.com/archives/C123/p1722"
    assert calls[0].get_method() == "GET"
    assert "channel=C123" in calls[0].full_url and "message_ts=1722.0042" in calls[0].full_url
