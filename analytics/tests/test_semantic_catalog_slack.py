import dataclasses

from semantic_catalog.records import MetricRecord
from semantic_catalog.slack_diff import changed_metric_names, diff_records, render_message


def _rec(name, definition="d", **kw):
    base = dict(
        name=name,
        label=name.title(),
        definition=definition,
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner="semantic-layer-business",
        detail_doc="engagement.md",
        retired=None,
        yaml_file="sem.yml",
        kind="metric",
        business_rule="counts: a user",
        anchored_on="event=E",
        measure="user_count",
    )
    base.update(kw)
    return MetricRecord(**base)


def test_diff_detects_added_removed_and_says_which_layer_changed():
    before = [_rec("a", "old def"), _rec("gone", "x")]
    after = [_rec("a", "new def"), _rec("b", "brand new")]
    lines = "\n".join(diff_records(before, after))
    assert "• added: b" in lines
    assert "• removed: gone" in lines
    # A prose edit is named as prose and says it affects no sign-off, rather
    # than reading as a change to the metric.
    assert "• wording: a (description updated, no sign-off affected)" in lines
    # Definition TEXT never appears in the thread; the PR diff owns the wording.
    assert "old def" not in lines and "new def" not in lines and "brand new" not in lines


def test_diff_names_the_anchor_change_that_used_to_move_nothing():
    # The edit at the centre of this ticket: swapping which events feed a metric
    # changes who is counted, and under one seal it produced no line at all.
    before = [_rec("a")]
    after = [_rec("a", anchored_on="event=Replaced")]
    lines = "\n".join(diff_records(before, after))
    assert "• anchor: a — the events that feed it changed" in lines


def test_diff_separates_a_rule_change_from_a_build_change():
    before = [_rec("a")]
    lines = "\n".join(diff_records(before, [_rec("a", business_rule="counts: somebody else")]))
    assert "• rule: a — what the number counts changed" in lines
    lines = "\n".join(diff_records(before, [_rec("a", measure="other_count")]))
    assert "• build: a — how it is computed changed" in lines


def test_diff_detects_each_half_separately():
    before = [_rec("a")]
    after = [_rec("a", rule_approved="2026-07-24", build_approved="2026-09-01")]
    lines = "\n".join(diff_records(before, after))
    assert "• rule sign-off: a — pending → 2026-07-24" in lines
    assert "• build sign-off: a — pending → 2026-09-01" in lines


def test_diff_reports_a_stale_only_transition():
    # Correcting a mis-pasted seal is a sidecar-only edit: staleness flips with
    # no other field moving. changed_metric_names counts the metric as changed
    # (whole-record equality), so the summary has to say why or the anchor and
    # the body disagree.
    before = [_rec("a", build_approved="2026-07-24", build_stale=True)]
    after = [_rec("a", build_approved="2026-07-24")]
    assert changed_metric_names(before, after) == ["a"]
    lines = "\n".join(diff_records(before, after))
    assert lines == "• build sign-off: a — now current"


def test_diff_reports_a_sign_off_going_stale():
    before = [_rec("a", build_approved="2026-07-24")]
    after = [_rec("a", anchored_on="event=New", build_approved="2026-07-24", build_stale=True)]
    lines = "\n".join(diff_records(before, after))
    assert "• anchor: a" in lines
    assert "build sign-off: a — now stale" in lines


def test_a_seal_scheme_change_is_explained_once_not_per_metric():
    # Changing the seal re-stamps every fingerprint at once. Without this the
    # merge summary reads as several metrics breaking in one merge.
    before = [
        _rec(n, build_approved="2026-07-24", build_stale=True, seal_scheme="legacy") for n in ("a", "b")
    ]
    after = [_rec(n, build_approved="2026-07-24") for n in ("a", "b")]
    lines = diff_records(before, after)
    assert lines[0].startswith("• seals recomputed under a new scheme")
    assert not any("sign-off" in line for line in lines[1:])


def test_diff_reports_a_new_value_at_signing():
    before = [_rec("a", build_approved="2026-07-24")]
    after = [_rec("a", build_approved="2026-07-24", value_at_signing=941)]
    assert "• value at signing: a — none → 941" in "\n".join(diff_records(before, after))


def test_message_flags_a_missing_approval_the_change_needed():
    msg = render_message([], [_rec("a", "d")], "http://pr/1", {"data": True, "business": False})
    assert "http://pr/1" in msg
    assert ":warning: review coverage: data ✓ · business ✗" in msg


def test_message_confirms_complete_review():
    msg = render_message([], [_rec("a", "d")], "http://pr/1", {"data": True, "business": True})
    assert "review coverage: data ✓ · business ✓" in msg
    assert ":warning:" not in msg


def test_message_does_not_warn_about_a_lane_the_change_never_needed():
    # Without this, routing changes nothing: the merge summary would go on
    # warning about a business sign-off for a build-only change, and everyone
    # would learn to ignore the line.
    msg = render_message(
        [], [_rec("a", "d")], "http://pr/1", {"data": True, "business": False}, required=["data"]
    )
    assert "review coverage: data ✓ · business — (not required)" in msg
    assert ":warning:" not in msg


def test_message_still_warns_when_a_required_lane_is_missing():
    msg = render_message(
        [], [_rec("a", "d")], "http://pr/1", {"data": False, "business": True}, required=["data", "business"]
    )
    assert ":warning:" in msg


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
