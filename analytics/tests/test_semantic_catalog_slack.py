from semantic_catalog.records import MetricRecord
from semantic_catalog.slack_diff import diff_records, render_message


def _rec(name, definition, ratified=None):
    return MetricRecord(
        name=name,
        label=name.title(),
        definition=definition,
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner="semantic-layer-business",
        ratified=ratified,
        detail_doc="engagement.md",
        retired=None,
        yaml_file="sem.yml",
        kind="metric",
    )


def test_diff_detects_added_removed_changed():
    before = [_rec("a", "old def"), _rec("gone", "x")]
    after = [_rec("a", "new def"), _rec("b", "brand new")]
    lines = "\n".join(diff_records(before, after))
    assert "b" in lines and "added" in lines.lower()
    assert "gone" in lines and "removed" in lines.lower()
    assert "a" in lines and "old def" in lines and "new def" in lines


def test_diff_detects_ratification():
    before = [_rec("a", "d", ratified=None)]
    after = [_rec("a", "d", ratified="2026-07-24")]
    lines = "\n".join(diff_records(before, after))
    assert "ratified" in lines.lower() and "2026-07-24" in lines


def test_message_flags_incomplete_review():
    msg = render_message([], [_rec("a", "d")], "http://pr/1", {"data": True, "business": False})
    assert "http://pr/1" in msg
    assert "incomplete" in msg.lower() or "missing" in msg.lower()
    assert "business" in msg.lower()
    assert "business group" in msg.lower()
    assert "business groups" not in msg.lower()


def test_message_flags_incomplete_review_both_groups_pluralizes():
    msg = render_message([], [_rec("a", "d")], "http://pr/1", {"data": False, "business": False})
    assert "data, business groups" in msg.lower()


def test_message_confirms_complete_review():
    msg = render_message([], [_rec("a", "d")], "http://pr/1", {"data": True, "business": True})
    assert "both groups" in msg.lower() or "complete" in msg.lower()
