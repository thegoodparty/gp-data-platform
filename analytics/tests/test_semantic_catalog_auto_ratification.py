"""The auto-ratification hook's pure core: when coverage completed, what it
earns, and how that lands in the sidecar text.
"""

from semantic_catalog import ratifications
from semantic_catalog.composition import completed_at, completion_date
from semantic_catalog.records import MetricRecord

DATA = ["danpelota", "hhkarimi", "sanjayr1", "datawithtristan"]
BIZ = ["amanda847", "audrey-gp"]


def _review(login, state="APPROVED", at="2026-08-05T00:00:00Z"):
    return {"login": login, "state": state, "submitted_at": at}


def _rec(name, definition="def", ratified=None, stale=False, retired=None):
    return MetricRecord(
        name=name,
        label=name,
        definition=definition,
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner=None,
        ratified=ratified,
        detail_doc=None,
        retired=retired,
        yaml_file="sem_analytics__users_win.yml",
        kind="metric",
        ratified_stale=stale,
    )


# --- when coverage completed -------------------------------------------------


def test_completion_is_the_later_group_not_the_first_approval():
    # The real #749 shape: business approved first, data completed it the same
    # day. The recorded date is when the SECOND group landed.
    reviews = [
        _review("amanda847", at="2026-08-04T14:20:29Z"),
        _review("danpelota", at="2026-08-04T16:26:10Z"),
    ]
    assert completed_at(reviews, DATA, BIZ) == "2026-08-04T16:26:10Z"
    assert completion_date(reviews, DATA, BIZ) == "2026-08-04"


def test_completion_uses_each_groups_earliest_approval():
    # A third approver piling on later must not push the completion date out.
    reviews = [
        _review("amanda847", at="2026-08-04T23:27:19Z"),
        _review("danpelota", at="2026-08-05T02:07:22Z"),
        _review("hhkarimi", at="2026-08-09T09:00:00Z"),
    ]
    assert completion_date(reviews, DATA, BIZ) == "2026-08-05"


def test_no_completion_when_a_group_is_missing():
    # The real #708 shape: business only, no data approval ever.
    assert completion_date([_review("amanda847")], DATA, BIZ) is None


def test_bot_approval_cannot_complete_a_group():
    reviews = [_review("amanda847"), _review("delegate-reviewer[bot]")]
    assert completion_date(reviews, DATA, BIZ) is None


def test_superseded_approval_does_not_count():
    # Same reviewer approved, then later requested changes.
    reviews = [
        _review("amanda847"),
        _review("danpelota", at="2026-08-05T01:00:00Z"),
        _review("danpelota", state="CHANGES_REQUESTED", at="2026-08-05T02:00:00Z"),
    ]
    assert completion_date(reviews, DATA, BIZ) is None


# --- what a merge earns ------------------------------------------------------


def test_new_metric_earns_a_sign_off():
    earned = ratifications.ratified_by_merge([], [_rec("m")], "2026-08-07", 800)
    assert earned["m"].ratified == "2026-08-07"
    assert earned["m"].approved_by_pr == 800
    assert earned["m"].definition_sha == ratifications.definition_sha(_rec("m"))


def test_changed_definition_on_a_pending_metric_earns_a_sign_off():
    before = [_rec("m", definition="old")]
    after = [_rec("m", definition="new")]
    assert "m" in ratifications.ratified_by_merge(before, after, "2026-08-07", 800)


def test_untouched_pending_metric_in_a_touched_file_earns_nothing():
    # The rule that stops one metric's PR from ratifying its file-mates.
    before = [_rec("edited", definition="old"), _rec("bystander")]
    after = [_rec("edited", definition="new"), _rec("bystander")]
    earned = ratifications.ratified_by_merge(before, after, "2026-08-07", 800)
    assert set(earned) == {"edited"}


def test_already_ratified_metric_earns_nothing():
    before = [_rec("m", ratified="2026-08-01")]
    after = [_rec("m", definition="new", ratified="2026-08-01")]
    assert ratifications.ratified_by_merge(before, after, "2026-08-07", 800) == {}


def test_stale_sign_off_is_re_earned_on_the_new_definition():
    before = [_rec("m", definition="old", ratified="2026-08-01")]
    after = [_rec("m", definition="new", ratified="2026-08-01", stale=True)]
    earned = ratifications.ratified_by_merge(before, after, "2026-08-07", 800)
    assert earned["m"].ratified == "2026-08-07"
    assert earned["m"].definition_sha == ratifications.definition_sha(_rec("m", definition="new"))


def test_retired_metric_earns_nothing():
    after = [_rec("m", retired="2026-08-01")]
    assert ratifications.ratified_by_merge([], after, "2026-08-07", 800) == {}


# --- writing it into the sidecar --------------------------------------------

EXISTING = """\
# Ratification sign-offs (header comment that must survive).

activated_serve_users:
  # Both groups approved #765, which contained the restatement.
  ratified: 2026-08-05
  definition_sha: '5e87555'
  approved_by_pr: 765

# win_activated_users is deliberately absent, so it reads pending.
# win_activated_users:
#   ratified: <date>
"""


def _sign_off(date="2026-08-07", sha="abc1234", pr=800):
    return ratifications.Ratification(date, sha, pr)


def _loaded(tmp_path, text):
    """Read the written text back the way production does, not via raw yaml.

    `load` normalizes an unquoted date, which YAML hands back as datetime.date,
    into the string the records carry.
    """
    path = tmp_path / "ratifications.yml"
    path.write_text(text)
    return ratifications.load(path)


def test_upsert_appends_a_new_entry_and_preserves_every_comment(tmp_path):
    out = ratifications.upsert(EXISTING, "win_users", _sign_off())
    assert "header comment that must survive" in out
    assert "Both groups approved #765" in out
    assert "# win_activated_users:" in out
    loaded = _loaded(tmp_path, out)
    assert loaded["win_users"] == ratifications.Ratification("2026-08-07", "abc1234", 800)
    assert loaded["activated_serve_users"].approved_by_pr == 765


def test_upsert_edits_an_existing_entry_in_place_without_duplicating_the_key(tmp_path):
    # The stale case: the metric already has a block, so appending a second one
    # would make a duplicate key that YAML silently resolves to the last.
    out = ratifications.upsert(EXISTING, "activated_serve_users", _sign_off(pr=900))
    assert out.count("activated_serve_users:") == 1
    assert "Both groups approved #765" in out, "the human's note must survive a re-date"
    assert _loaded(tmp_path, out)["activated_serve_users"] == ratifications.Ratification(
        "2026-08-07", "abc1234", 900
    )


def test_upsert_adds_a_field_the_existing_entry_lacks(tmp_path):
    text = "m:\n  ratified: 2026-08-01\n  definition_sha: 'aaaaaaa'\n"
    out = ratifications.upsert(text, "m", _sign_off())
    assert _loaded(tmp_path, out)["m"].approved_by_pr == 800


def test_upsert_never_matches_a_commented_out_key(tmp_path):
    text = "# win_activated_users:\n#   ratified: <date>\n"
    out = ratifications.upsert(text, "win_activated_users", _sign_off())
    assert out.startswith("# win_activated_users:"), "the commented block stays commented"
    assert _loaded(tmp_path, out)["win_activated_users"].approved_by_pr == 800


def test_upsert_writes_an_all_digit_hash_quoted(tmp_path):
    # Unquoted, YAML reads it as an integer and the leading zero is gone.
    out = ratifications.upsert(EXISTING, "win_users", _sign_off(sha="0123456"))
    assert _loaded(tmp_path, out)["win_users"].definition_sha == "0123456"
