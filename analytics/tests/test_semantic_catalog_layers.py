"""The canonical renderings the two seals are computed from.

A seal is only as good as the stability of the text under it: two YAML
authorings that mean the same thing must render identically, or a reorder
expires an approval nobody changed.
"""

import pytest
from semantic_catalog import layers

RULE = {
    "counts": "A user counts once the product has sent outreach for them.",
    "excludes": ["Self-reported outreach.", "Preparation."],
    "known_gaps": ["Phone banking has no in-product send."],
}


def test_a_rule_renders_every_part():
    out = layers.render_business_rule(RULE, "m")
    assert out.startswith("counts: A user counts once")
    assert "excludes: Self-reported outreach. ~ Preparation." in out
    assert "known_gaps: Phone banking has no in-product send." in out


def test_rewrapping_a_block_scalar_does_not_move_the_rule():
    wrapped = {**RULE, "counts": "A user counts once the product\nhas sent outreach   for them."}
    assert layers.render_business_rule(wrapped, "m") == layers.render_business_rule(RULE, "m")


def test_absent_optional_lists_render_as_empty_not_as_missing():
    # Deleting the last exclusion IS a change to the rule and must move the seal,
    # so the key has to be rendered either way rather than dropping out.
    with_none = layers.render_business_rule({"counts": "x"}, "m")
    with_one = layers.render_business_rule({"counts": "x", "excludes": ["y"]}, "m")
    assert "excludes:" in with_none and with_none != with_one


def test_no_rule_is_none_not_an_error():
    assert layers.render_business_rule(None, "m") is None
    assert layers.render_business_rule({}, "m") is None


def test_a_rule_without_counts_is_rejected():
    with pytest.raises(ValueError, match="counts"):
        layers.render_business_rule({"excludes": ["y"]}, "m")


def test_an_unknown_rule_key_is_rejected():
    # An open mapping would let the implementation layer drift back in one key
    # at a time, which is the collapse the split exists to prevent.
    with pytest.raises(ValueError, match="unknown key"):
        layers.render_business_rule({"counts": "x", "events": ["E"]}, "m")


def test_a_string_where_a_list_belongs_is_rejected():
    # YAML makes this easy to get wrong, and it would silently render one
    # character per exclusion.
    with pytest.raises(ValueError, match="list of lines"):
        layers.render_business_rule({"counts": "x", "excludes": "y"}, "m")


def test_an_empty_exclusion_line_is_rejected():
    with pytest.raises(ValueError, match="empty line"):
        layers.render_business_rule({"counts": "x", "excludes": ["y", "  "]}, "m")


ANCHORS = [
    {"event": "Viewed", "path": "/dashboard"},
    {"event": "VO - Completed", "excluding": {"method": "manual"}},
    {"event": "Old Event", "era": "historical"},
]


def test_an_anchor_renders_every_qualifier():
    out = layers.render_anchored_on(ANCHORS, "m")
    assert "event=Viewed path=/dashboard" in out
    assert "event=VO - Completed excluding=method=manual" in out
    assert "event=Old Event era=historical" in out


def test_excluding_values_normalise_to_a_stable_order():
    # The excluded values are a set. A YAML reorder meaning the same thing must
    # not move the build seal.
    a = layers.render_anchored_on([{"event": "E", "excluding": {"m": ["native", "manual"]}}], "m")
    b = layers.render_anchored_on([{"event": "E", "excluding": {"m": ["manual", "native"]}}], "m")
    assert a == b


def test_excluding_properties_normalise_to_a_stable_order():
    a = layers.render_anchored_on([{"event": "E", "excluding": {"b": 1, "a": 2}}], "m")
    b = layers.render_anchored_on([{"event": "E", "excluding": {"a": 2, "b": 1}}], "m")
    assert a == b


def test_leg_order_is_significant():
    # Unlike the excluded values, the legs are an authored list: reordering them
    # is a diff a reviewer sees, so it is not normalised away.
    assert layers.render_anchored_on(ANCHORS, "m") != layers.render_anchored_on(list(reversed(ANCHORS)), "m")


def test_an_era_change_moves_the_anchor():
    # A historical leg is not decoration: dropping the marker changes which rows
    # the compiled predicate matches for past dates.
    live = [{"event": "E"}]
    assert layers.render_anchored_on(live, "m") != layers.render_anchored_on(
        [{"event": "E", "era": "historical"}], "m"
    )


def test_a_leg_without_an_event_is_rejected():
    with pytest.raises(ValueError, match="event"):
        layers.render_anchored_on([{"path": "/x"}], "m")


def test_a_rule_naming_a_declared_event_is_caught():
    # The business group rules on what the number means. They cannot be asked
    # which of three moments sharing one event name is the send, and a rule that
    # names an event has swallowed that question, so the two layers can no
    # longer go stale independently.
    rule = "counts: a user counts on VO - Completed | excludes:  | known_gaps: "
    assert layers.rule_names_an_implementation(rule, ANCHORS) == ["VO - Completed"]


def test_a_rule_naming_no_event_is_clean():
    assert layers.rule_names_an_implementation(layers.render_business_rule(RULE, "m"), ANCHORS) == []
    assert layers.rule_names_an_implementation(None, ANCHORS) == []
