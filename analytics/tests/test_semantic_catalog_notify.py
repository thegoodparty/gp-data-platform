"""The degrade notice must reach a person, or say plainly what stopped it.

Every outcome string here is read by someone deciding what to fix, and each
wrong guess sends them somewhere different: a refused DM means a Slack
permission, an unset owner means an env var, an untried channel means neither.
So the matrix is pinned whole rather than case by case — claiming an attempt
that never happened is the same class of lie as a guard reporting green while
it did not run.
"""

import http.client

import pytest
from semantic_catalog import notify

PROBLEMS = ["OMNI_READ_TOKEN is not set. Build approvals are NOT being checked."]


def _recorder(refuse=()):
    sent = []

    def post(token, channel, text):
        sent.append((channel, text))
        return channel not in refuse

    return sent, post


def test_a_clean_run_says_nothing():
    # Silence is correct only when the check actually ran.
    sent, post = _recorder()
    assert "nothing to report" in notify.notify([], token="t", owner="U1", channel="C1", post=post)
    assert sent == []


def test_no_token_is_named_as_the_reason():
    sent, post = _recorder()
    assert notify.TOKEN_ENV in notify.notify(PROBLEMS, token="", owner="U1", channel="C1", post=post)
    assert sent == []


# owner, channel, refused, expected attempts, substrings that must appear, and
# substrings that must NOT — the second list is the point: every historical bug
# in this function was claiming something it had not done.
MATRIX = [
    ("U1", "C1", (), ["U1"], ["sent to the owner"], ["refused", "channel"]),
    ("U1", "C1", ("U1",), ["U1", "C1"], ["owner DM was refused", "sent to the channel"], ["not delivered"]),
    ("U1", "C1", ("U1", "C1"), ["U1", "C1"], ["owner DM was refused", "channel post failed"], []),
    ("U1", "", ("U1",), ["U1"], ["owner DM was refused", "no SLACK_CHANNEL_ID"], ["channel post failed"]),
    ("", "C1", (), ["C1"], ["no SLACK_OWNER_DM", "sent to the channel"], ["refused"]),
    ("", "C1", ("C1",), ["C1"], ["no SLACK_OWNER_DM", "channel post failed"], ["refused"]),
    ("", "", (), [], ["no SLACK_OWNER_DM", "no SLACK_CHANNEL_ID"], ["refused", "channel post failed"]),
]


@pytest.mark.parametrize("owner,channel,refuse,attempts,must,must_not", MATRIX)
def test_the_outcome_describes_only_what_was_attempted(owner, channel, refuse, attempts, must, must_not):
    sent, post = _recorder(refuse=set(refuse))
    outcome = notify.notify(PROBLEMS, token="t", owner=owner, channel=channel, post=post)
    assert [target for target, _ in sent] == attempts
    for fragment in must:
        assert fragment in outcome, outcome
    for fragment in must_not:
        assert fragment not in outcome, outcome


def test_only_a_real_refusal_is_reported_to_the_channel():
    # The channel post carries the "DM was refused" note only when one was. With
    # no owner set it would send a reader hunting a Slack permission problem
    # that is really a missing env var.
    sent, post = _recorder()
    notify.notify(PROBLEMS, token="t", owner="", channel="C1", post=post)
    assert "refused" not in sent[0][1]
    sent, post = _recorder(refuse={"U1"})
    notify.notify(PROBLEMS, token="t", owner="U1", channel="C1", post=post)
    assert "refused" in sent[1][1]


def test_a_connection_dropped_mid_body_does_not_escape(monkeypatch):
    # IncompleteRead descends from HTTPException, not OSError, so it slipped the
    # original except clause and broke the never-raises contract — failing a
    # publish that also opens the ratification PR, over a blip that says nothing
    # about whether the merge was sound. Patched at urlopen so the real _post runs.
    def dropped(request, timeout=None):
        raise http.client.IncompleteRead(b"partial")

    monkeypatch.setattr(notify.urllib.request, "urlopen", dropped)
    assert notify._post("t", "C1", "text") is False


def test_the_notice_carries_every_problem_and_how_it_clears():
    text = notify.render(["first thing", "second thing"])
    assert "- first thing" in text and "- second thing" in text
    assert "ORG_READ_TOKEN" in text


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def read(self):
        return self._payload.encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.mark.parametrize("payload,expected", [('{"ok": true}', True), ('{"ok": false, "error": "x"}', False)])
def test_post_reads_the_body_not_the_status(monkeypatch, payload, expected):
    # chat.postMessage answers HTTP 200 with {"ok": false} for a bad channel, a
    # missing scope or bad auth. Trusting the status would report every one of
    # those as delivered, which is the silent failure this module exists to stop.
    monkeypatch.setattr(notify.urllib.request, "urlopen", lambda request, timeout=None: _Response(payload))
    assert notify._post("t", "C1", "text") is expected
