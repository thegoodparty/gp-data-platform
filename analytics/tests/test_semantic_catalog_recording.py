from semantic_catalog import ratifications, recording
from semantic_catalog.records import MetricRecord

SIDECAR = """\
# Header comment that must survive.

win_users:
  # A human's note about this sign-off.
  ratified: 2026-08-04
  definition_sha: '865d003'
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
        ratified=None,
        detail_doc=None,
        retired=None,
        yaml_file="sem_analytics__users_win.yml",
        kind="metric",
    )


def _earned(name="serve_users", date="2026-08-07", sha="abc1234", pr=800):
    return {name: ratifications.Ratification(date, sha, pr)}


def test_apply_writes_each_earned_entry_with_a_provenance_note(tmp_path):
    out = recording.apply(SIDECAR, _earned(), 800)
    path = tmp_path / "r.yml"
    path.write_text(out)
    loaded = ratifications.load(path)
    assert loaded["serve_users"] == ratifications.Ratification("2026-08-07", "abc1234", 800)
    assert "#800" in out, "the entry must say where the sign-off came from"
    assert "both review groups approved" in out


def test_apply_preserves_existing_comments_and_entries(tmp_path):
    out = recording.apply(SIDECAR, _earned(), 800)
    assert "Header comment that must survive" in out
    assert "A human's note about this sign-off" in out
    path = tmp_path / "r.yml"
    path.write_text(out)
    assert ratifications.load(path)["win_users"].approved_by_pr == 749


def test_apply_writes_entries_in_a_stable_order():
    # Two metrics earned by one merge must land in a deterministic order, or a
    # re-run produces a different diff and the branch churns.
    earned = {
        "b_metric": ratifications.Ratification("2026-08-07", "bbbbbbb", 800),
        "a_metric": ratifications.Ratification("2026-08-07", "aaaaaaa", 800),
    }
    out = recording.apply(SIDECAR, earned, 800)
    assert out.index("a_metric:") < out.index("b_metric:")


def test_apply_is_idempotent():
    once = recording.apply(SIDECAR, _earned(), 800)
    twice = recording.apply(once, _earned(), 800)
    assert once == twice


def test_manifest_carries_what_the_pr_body_needs():
    earned = _earned()
    got = recording.manifest(earned, [_rec("serve_users", label="Serve Users")], "2026-08-07", 800)
    assert got["pr"] == 800
    assert got["date"] == "2026-08-07"
    assert got["metrics"] == [{"name": "serve_users", "label": "Serve Users", "definition_sha": "abc1234"}]


def test_manifest_is_empty_when_nothing_was_earned():
    got = recording.manifest({}, [], None, 800)
    assert got["metrics"] == []
