from semantic_catalog import pr_marker
from semantic_catalog.pr_marker import ThreadState


def _state(**kw):
    base = dict(ts="1722.0001", channel="C08K22X9ZNH", permalink="https://gp.slack.com/p1722")
    base.update(kw)
    return ThreadState(**base)


def test_round_trip():
    s = _state(announced={"data": "approved", "business": None}, pr_state="open", merged=False)
    body = pr_marker.render(s)
    assert pr_marker.parse(body) == s


def test_render_has_visible_link_and_hidden_json():
    body = pr_marker.render(_state())
    assert body.startswith("Slack governance thread: https://gp.slack.com/p1722")
    assert "<!-- semantic-layer-thread v1" in body and body.rstrip().endswith("-->")


def test_parse_returns_none_without_marker():
    assert pr_marker.parse("just a human comment") is None


def test_parse_returns_none_on_corrupt_json():
    body = "<!-- semantic-layer-thread v1\n{not json\n-->"
    assert pr_marker.parse(body) is None


def test_defaults():
    s = _state()
    assert s.announced == {"data": None, "business": None}
    assert s.pr_state == "open" and s.merged is False


def test_is_marker():
    assert pr_marker.is_marker(pr_marker.render(_state()))
    assert not pr_marker.is_marker("unrelated bot comment")
