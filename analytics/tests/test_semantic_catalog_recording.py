import pytest
from semantic_catalog import ratifications, recording
from semantic_catalog.records import MetricRecord

SIDECAR = """\
# Header comment that must survive.

win_users:
  # A human's note about this sign-off.
  business:
    approved: 2026-07-29
    rule_sha: '865d003'
  data:
    approved: 2026-08-04
    build_sha: '1213bea'
    value_at_signing: 63744
  approved_by_pr: 749
"""


def _rec(name, definition="def", label=None):
    return MetricRecord(
        name=name,
        label=label or name,
        definition=definition,
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner=None,
        detail_doc=None,
        retired=None,
        yaml_file="sem_analytics__users_win.yml",
        kind="metric",
    )


def _earned(name="serve_users", pr=800, rule=True, data=True):
    return {
        name: ratifications.Ratification(
            rule=ratifications.SignOff("2026-08-07", "abc1234") if rule else None,
            data=ratifications.SignOff("2026-08-09", "def5678", 1878) if data else None,
            approved_by_pr=pr,
        )
    }


def test_apply_writes_each_earned_entry_with_a_provenance_note(tmp_path):
    out = recording.apply(SIDECAR, _earned(), 800)
    path = tmp_path / "r.yml"
    path.write_text(out)
    loaded = ratifications.load(path)["serve_users"]
    assert loaded.rule.approved == "2026-08-07" and loaded.rule.sha == "abc1234"
    assert loaded.data.approved == "2026-08-09" and loaded.data.value == 1878
    assert loaded.approved_by_pr == 800
    assert "#800" in out, "the entry must say where the sign-off came from"


def test_apply_preserves_existing_comments_and_entries(tmp_path):
    out = recording.apply(SIDECAR, _earned(), 800)
    assert "Header comment that must survive" in out
    assert "A human's note about this sign-off" in out
    path = tmp_path / "r.yml"
    path.write_text(out)
    assert ratifications.load(path)["win_users"].approved_by_pr == 749


def test_recording_one_half_leaves_the_other_alone(tmp_path):
    # The case lane routing creates: a data-only change records a build half on
    # a PR the business group was never asked about. Restating or dropping their
    # rule approval, given months earlier on another PR, would be a lie either way.
    out = recording.apply(SIDECAR, _earned(name="win_users", rule=False), 800)
    path = tmp_path / "r.yml"
    path.write_text(out)
    entry = ratifications.load(path)["win_users"]
    assert entry.rule.approved == "2026-07-29" and entry.rule.sha == "865d003"
    assert entry.data.approved == "2026-08-09" and entry.data.value == 1878


def test_apply_writes_entries_in_a_stable_order():
    # Two metrics earned by one merge must land in a deterministic order, or a
    # re-run produces a different diff and the branch churns.
    earned = {**_earned("b_metric"), **_earned("a_metric")}
    out = recording.apply(SIDECAR, earned, 800)
    assert out.index("a_metric:") < out.index("b_metric:")


def test_apply_is_idempotent():
    once = recording.apply(SIDECAR, _earned(), 800)
    twice = recording.apply(once, _earned(), 800)
    assert once == twice


def test_declared_values_reads_the_pr_bodys_markers():
    # CI cannot compute a metric's value, so the author states it where
    # reviewers read it and the recorder takes it from there.
    body = (
        "Swapped the outreach terminal.\n"
        "<!-- semantic-value: win_activated_users = 1,260 -->\n"
        "<!-- semantic-value: win_users = 63744 -->\n"
    )
    assert recording.declared_values(body) == {"win_activated_users": 1260, "win_users": 63744}


def test_declared_values_is_empty_for_a_body_with_no_markers():
    assert recording.declared_values("just some prose") == {}
    assert recording.declared_values("") == {}


def test_a_corrected_value_wins_over_the_one_above_it():
    # An edited PR body usually keeps the superseded number above the fix.
    body = "<!-- semantic-value: m = 100 -->\nCorrection:\n<!-- semantic-value: m = 120 -->"
    assert recording.declared_values(body) == {"m": 120}


def test_unvalued_names_the_build_halves_that_cannot_be_recorded():
    assert recording.unvalued(["a", "b"], {"b": 5}) == ["a"]
    assert recording.unvalued(["a"], {"a": 5}) == []


def test_manifest_carries_both_halves():
    got = recording.manifest(_earned(), [_rec("serve_users", label="Serve Users")], 800)
    assert got["pr"] == 800
    assert got["metrics"][0]["label"] == "Serve Users"
    assert got["metrics"][0]["rule"] == {"approved": "2026-08-07", "sha": "abc1234"}
    assert got["metrics"][0]["data"]["value_at_signing"] == 1878


def test_manifest_is_empty_when_nothing_was_earned():
    assert recording.manifest({}, [], 800)["metrics"] == []


def test_pr_body_names_each_half_with_its_own_date():
    body = recording.pr_body(
        recording.manifest(_earned(), [_rec("serve_users", label="Serve Users")], 800),
        repo="thegoodparty/gp-data-platform",
    )
    assert "serve_users" in body
    assert "2026-08-07" in body and "2026-08-09" in body
    assert "1878" in body
    assert "thegoodparty/gp-data-platform/pull/800" in body


def test_pr_body_explains_what_the_approver_is_agreeing_to():
    body = recording.pr_body(recording.manifest(_earned(), [], 800), repo="o/r")
    assert "matches the review record" in body.lower()


def test_pr_body_is_pure_ascii():
    # DATA-2211 copy rule, and this text is machine-generated so nothing else
    # catches a violation. Pure ASCII is the actual constraint: it rules out
    # em dash, en dash, curly quotes, ellipsis, and emoji in one assertion,
    # unlike a codepoint ceiling that some of those slip under.
    body = recording.pr_body(recording.manifest(_earned(), [], 800), repo="o/r")
    assert body.isascii()


def test_pr_body_raises_when_nothing_was_earned():
    # A bad write must fail loudly, not open a PR whose table has no rows.
    with pytest.raises(ValueError):
        recording.pr_body({"pr": 800, "metrics": []}, repo="o/r")
