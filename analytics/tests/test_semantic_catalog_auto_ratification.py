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


def _rec(name, definition="def", ratified=None, stale=False, retired=None, owner=None):
    return MetricRecord(
        name=name,
        label=name,
        definition=definition,
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner=owner,
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


def test_changes_requested_is_superseded_by_a_later_approval():
    # The other direction of "latest review counts": address feedback and
    # re-approve, and the earlier rejection must not keep the group uncovered.
    reviews = [
        _review("amanda847"),
        _review("danpelota", state="CHANGES_REQUESTED", at="2026-08-05T01:00:00Z"),
        _review("danpelota", at="2026-08-05T02:00:00Z"),
    ]
    assert completion_date(reviews, DATA, BIZ) == "2026-08-05"


def test_bot_listed_as_a_group_member_still_cannot_complete_coverage():
    # "Bot accounts never count toward a group, even if listed as a member":
    # a misconfigured team roster containing a bot login must not let the
    # bot's own approval satisfy that group.
    reviews = [_review("amanda847"), _review("delegate-reviewer[bot]")]
    data_with_bot_as_member = [*DATA, "delegate-reviewer[bot]"]
    assert completion_date(reviews, data_with_bot_as_member, BIZ) is None


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


def test_untouched_stale_metric_in_a_touched_file_earns_nothing():
    # Staleness can predate this merge entirely (an earlier hand-edit changed
    # the definition without a re-ratification). The file-mates rule applies
    # just as much to a stale bystander as to a pending one: no movement in
    # THIS merge means this PR's reviewers never saw it.
    before = [
        _rec("edited", definition="old"),
        _rec("stale_bystander", ratified="2026-08-01", stale=True),
    ]
    after = [
        _rec("edited", definition="new"),
        _rec("stale_bystander", ratified="2026-08-01", stale=True),
    ]
    earned = ratifications.ratified_by_merge(before, after, "2026-08-07", 800)
    assert set(earned) == {"edited"}


def test_metadata_only_change_does_not_earn_a_sign_off():
    # owner/detail_doc are deliberately excluded from FINGERPRINT_FIELDS, so
    # editing only owner is not "the definition moved" -- no one reviewed a
    # changed definition, so a pending metric touched only this way earns
    # nothing.
    before = [_rec("m", owner="alice")]
    after = [_rec("m", owner="bob")]
    assert ratifications.ratified_by_merge(before, after, "2026-08-07", 800) == {}


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


def test_upsert_editing_an_entry_preserves_an_unrelated_trailing_comment_block(tmp_path):
    # The edit path's block-end detection sweeps trailing comments (and blank
    # lines) after the last rewritten field into the block being edited. It is
    # correct today only because nothing in the rewrite loop touches a line
    # that doesn't match a written field or the auto-note prefix -- a
    # regression here would silently drop or reorder someone else's sidecar
    # comment. Compare byte for byte, not just substring-contains.
    trailing = EXISTING[EXISTING.index("\n# win_activated_users") :]
    out = ratifications.upsert(EXISTING, "activated_serve_users", _sign_off(pr=900))
    assert out.endswith(trailing)


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


def test_upsert_into_an_empty_file_creates_a_single_loadable_entry(tmp_path):
    # The very first ratification ever: no sidecar content exists yet.
    out = ratifications.upsert("", "m", _sign_off())
    assert _loaded(tmp_path, out) == {"m": ratifications.Ratification("2026-08-07", "abc1234", 800)}


def test_upsert_with_a_note_adds_a_comment_that_survives_the_round_trip(tmp_path):
    # Comments are load-bearing: a note passed alongside a brand-new entry
    # must both appear in the text, tagged with the auto-note prefix so
    # recording.py can find and replace its own notes later, and not break
    # parsing.
    out = ratifications.upsert(EXISTING, "win_users", _sign_off(), note="Approved in #800.")
    assert f"# {ratifications.AUTO_NOTE_PREFIX}Approved in #800." in out
    assert _loaded(tmp_path, out)["win_users"] == ratifications.Ratification("2026-08-07", "abc1234", 800)


def test_upsert_with_a_note_lands_on_an_edit(tmp_path):
    # A re-earned stale sign-off already has a block, so it takes the edit
    # path -- the note must not be silently dropped there.
    text = "m:\n  ratified: 2026-08-01\n  definition_sha: 'aaaaaaa'\n  approved_by_pr: 100\n"
    out = ratifications.upsert(text, "m", _sign_off(pr=200), note="Re-earned in #900.")
    assert f"# {ratifications.AUTO_NOTE_PREFIX}Re-earned in #900." in out
    assert _loaded(tmp_path, out)["m"] == ratifications.Ratification("2026-08-07", "abc1234", 200)


def test_upsert_with_a_note_replaces_the_prior_auto_note_on_re_edit(tmp_path):
    # Re-recording must not accumulate a second auto-note each time a metric
    # goes stale and gets re-earned.
    text = "m:\n  ratified: 2026-08-01\n  definition_sha: 'aaaaaaa'\n  approved_by_pr: 100\n"
    once = ratifications.upsert(text, "m", _sign_off(pr=200), note="First re-earn, PR #900.")
    twice = ratifications.upsert(once, "m", _sign_off(pr=300), note="Second re-earn, PR #901.")
    assert twice.count(ratifications.AUTO_NOTE_PREFIX) == 1
    assert "First re-earn" not in twice
    assert f"# {ratifications.AUTO_NOTE_PREFIX}Second re-earn, PR #901." in twice
    assert _loaded(tmp_path, twice)["m"].approved_by_pr == 300


def test_upsert_with_a_note_leaves_a_human_comment_in_the_same_block_untouched(tmp_path):
    # The auto-note and a human's hand-written reasoning must coexist: only
    # the prefixed line is ours to replace.
    out = ratifications.upsert(
        EXISTING, "activated_serve_users", _sign_off(pr=900), note="Re-earned in #900."
    )
    assert "Both groups approved #765" in out
    assert f"# {ratifications.AUTO_NOTE_PREFIX}Re-earned in #900." in out
    assert _loaded(tmp_path, out)["activated_serve_users"].approved_by_pr == 900


def test_upsert_writes_a_missing_pr_number_as_yaml_null(tmp_path):
    # approved_by_pr is optional on Ratification (a hand-authored entry may
    # lack a PR). Writing the bare word `None` would round-trip through
    # load() as the STRING "None", not Python None, silently corrupting the
    # type `load()` and the rest of the code expect.
    sign_off = ratifications.Ratification("2026-08-07", "abc1234", None)
    out = ratifications.upsert("", "m", sign_off)
    assert _loaded(tmp_path, out)["m"].approved_by_pr is None


def test_upsert_editing_to_a_missing_pr_number_writes_null(tmp_path):
    # Same bug, the other code path: rewriting an existing field line rather
    # than composing a brand-new block.
    text = "m:\n  ratified: 2026-08-01\n  definition_sha: 'aaaaaaa'\n  approved_by_pr: 100\n"
    out = ratifications.upsert(text, "m", ratifications.Ratification("2026-08-07", "abc1234", None))
    assert _loaded(tmp_path, out)["m"].approved_by_pr is None
