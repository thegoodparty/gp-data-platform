"""The degrade notice must reach a person, or say plainly that it did not."""

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


def test_the_notice_goes_to_the_owner_not_the_channel():
    # #data-alignment is for metric news; a token the review groups cannot
    # provision is not news they can act on.
    sent, post = _recorder()
    assert "sent to the owner" in notify.notify(PROBLEMS, token="t", owner="U1", channel="C1", post=post)
    assert [channel for channel, _ in sent] == ["U1"]


def test_a_refused_dm_falls_back_to_the_channel():
    # A missing im:write would otherwise drop the notice silently, which is the
    # exact failure this exists to remove.
    sent, post = _recorder(refuse={"U1"})
    outcome = notify.notify(PROBLEMS, token="t", owner="U1", channel="C1", post=post)
    assert "sent to the channel instead" in outcome
    assert [channel for channel, _ in sent] == ["U1", "C1"]
    assert "refused" in sent[1][1]


def test_both_refused_is_reported_rather_than_swallowed():
    # The caller annotates instead of failing: the only way both are refused is a
    # dead Slack token, which the weekly token-health workflow already fails on.
    sent, post = _recorder(refuse={"U1", "C1"})
    assert "refused both" in notify.notify(PROBLEMS, token="t", owner="U1", channel="C1", post=post)
    assert len(sent) == 2


def test_no_token_is_named_as_the_reason():
    sent, post = _recorder()
    assert notify.TOKEN_ENV in notify.notify(PROBLEMS, token="", owner="U1", channel="C1", post=post)
    assert sent == []


def test_the_notice_carries_every_problem_and_how_it_clears():
    text = notify.render(["first thing", "second thing"])
    assert "- first thing" in text and "- second thing" in text
    assert "ORG_READ_TOKEN" in text
