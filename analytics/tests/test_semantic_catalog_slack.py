import dataclasses

from semantic_catalog.records import MetricRecord
from semantic_catalog.slack_diff import changed_metric_names, diff_records, render_message


def _rec(name, definition="d", ratified=None, **kw):
    base = dict(
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
    base.update(kw)
    return MetricRecord(**base)


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
    assert ":warning: review coverage: data ✓ · business ✗" in msg


def test_message_flags_incomplete_review_both_groups_pluralizes():
    msg = render_message([], [_rec("a", "d")], "http://pr/1", {"data": False, "business": False})
    assert ":warning: review coverage: data ✗ · business ✗" in msg


def test_message_confirms_complete_review():
    msg = render_message([], [_rec("a", "d")], "http://pr/1", {"data": True, "business": True})
    assert "review coverage: data ✓ · business ✓" in msg
    assert ":warning:" not in msg


def test_diff_detects_retired_and_owner_changes():
    base = _rec("a", "d")
    after = dataclasses.replace(base, retired="2026-07-01", owner="semantic-layer-data")
    lines = "\n".join(diff_records([base], [after]))
    assert "retired: a" in lines and "2026-07-01" in lines
    assert "owner: a" in lines and "semantic-layer-data" in lines


def test_changed_metric_names_added_removed_changed():
    before = [_rec("kept"), _rec("gone"), _rec("edited", definition="old")]
    after = [_rec("kept"), _rec("new"), _rec("edited", definition="new")]
    assert changed_metric_names(before, after) == ["edited", "gone", "new"]


def test_changed_metric_names_empty_when_identical():
    recs = [_rec("a"), _rec("b")]
    assert changed_metric_names(recs, recs) == []


def test_render_message_coverage_is_one_line_complete():
    msg = render_message([], [_rec("a")], "http://pr", {"data": True, "business": True})
    assert "review coverage: data ✓ · business ✓" in msg
    assert ":warning:" not in msg


def test_render_message_coverage_warns_when_incomplete():
    msg = render_message([], [_rec("a")], "http://pr", {"data": True, "business": False})
    assert ":warning: review coverage: data ✓ · business ✗" in msg


def test_changed_metric_names_ignores_absolute_path_differences():
    # The before-set is parsed from a temp worktree, so yaml_file differs by
    # directory on every record even when nothing changed. Whole-record
    # equality used to report the entire catalog as changed; the anchor then
    # listed every metric in the layer instead of the one under review.
    before = [_rec("a", yaml_file="/tmp/base/dbt/project/models/marts/analytics/sem_x.yml")]
    after = [_rec("a", yaml_file="/home/runner/work/repo/repo/dbt/project/models/marts/analytics/sem_x.yml")]
    assert changed_metric_names(before, after) == []


def test_changed_metric_names_still_flags_a_move_between_sem_files():
    # Basename normalization must not swallow a genuine relocation.
    before = [_rec("a", yaml_file="/tmp/base/dbt/project/models/marts/analytics/sem_x.yml")]
    after = [_rec("a", yaml_file="/checkout/dbt/project/models/marts/analytics/sem_y.yml")]
    assert changed_metric_names(before, after) == ["a"]
