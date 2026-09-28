import json

from semantic_catalog.notices import CHANNEL, OWNER, SKIP, main, route


def _classified(business=(), data=(), unreviewed=()):
    """A `--classify-lanes` payload from the branch that actually diffed."""
    return {
        "business": list(business),
        "data": list(data),
        "unreviewed": list(unreviewed),
        "teams": [],
        "summary": "irrelevant to routing",
    }


def test_rule_change_goes_to_the_channel():
    assert route(_classified(business=["win_users"]))["target"] == CHANNEL


def test_build_change_goes_to_the_channel():
    assert route(_classified(data=["win_users"]))["target"] == CHANNEL


def test_nothing_moved_tells_nobody():
    assert route(_classified())["target"] == SKIP


def test_mechanics_label_downgrades_to_the_owner_rather_than_silencing():
    # The label must never be able to delete a metric announcement outright:
    # a false mechanics claim has to stay visible to someone who would catch it.
    verdict = route(_classified(business=["win_users"]), mechanics_label=True)
    assert verdict["target"] == OWNER


def test_mechanics_label_on_an_empty_diff_still_skips():
    assert route(_classified(), mechanics_label=True)["target"] == SKIP


def test_display_prose_only_goes_to_the_owner():
    # Asks nobody for review, but a metric's description did move, so it is not
    # dev work either.
    assert route(_classified(unreviewed=["win_users"]))["target"] == OWNER


def test_no_base_tree_posts_to_the_channel():
    assert route(None)["target"] == CHANNEL


def test_unclassifiable_lanes_are_not_read_as_nothing_moved():
    # `--classify-lanes` with no base tree emits empty lane lists and no
    # `summary`. Reading that as "nothing moved" would suppress a real post.
    no_base = {
        "business": [],
        "data": [],
        "unreviewed": [],
        "teams": ["semantic-layer-data", "semantic-layer-business"],
        "reason": "no base tree to diff against; requesting both groups",
    }
    assert route(no_base)["target"] == CHANNEL


def test_main_prints_route_json(capsys, monkeypatch):
    monkeypatch.setenv("LANES", json.dumps(_classified(data=["win_users"])))
    monkeypatch.setenv("MECHANICS_LABEL", "true")

    assert main() == 0

    assert json.loads(capsys.readouterr().out)["target"] == OWNER


def test_main_treats_an_absent_lanes_env_as_unclassifiable(capsys, monkeypatch):
    monkeypatch.delenv("LANES", raising=False)
    monkeypatch.delenv("MECHANICS_LABEL", raising=False)

    assert main() == 0

    assert json.loads(capsys.readouterr().out)["target"] == CHANNEL
