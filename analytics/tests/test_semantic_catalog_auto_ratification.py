"""The auto-ratification hook's pure core: when coverage completed, what it
earns, and how that lands in the sidecar text.
"""

from semantic_catalog import lanes, ratifications
from semantic_catalog.composition import completed_at, completion_date, group_dates
from semantic_catalog.records import MetricRecord

DATA = ["danpelota", "hhkarimi", "sanjayr1", "datawithtristan"]
BIZ = ["amanda847", "audrey-gp"]


def _review(login, state="APPROVED", at="2026-08-05T00:00:00Z"):
    return {"login": login, "state": state, "submitted_at": at}


def _rec(name, definition="def", retired=None, owner=None, rule="counts: a user", anchor="event=E", **kw):
    return MetricRecord(
        name=name,
        label=name,
        definition=definition,
        metric_type="simple",
        source="ref('m')",
        dimensions=(),
        filter=None,
        owner=owner,
        detail_doc=None,
        retired=retired,
        yaml_file="sem_analytics__users_win.yml",
        kind="metric",
        business_rule=rule,
        anchored_on=anchor,
        measure="user_count",
        **kw,
    )


BOTH = {"data": "2026-08-07", "business": "2026-08-07"}
VALUES = {"m": 100, "edited": 100, "bystander": 100, "stale_bystander": 100}


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


# --- each group's own date ---------------------------------------------------


def test_group_dates_reports_each_group_independently():
    # What lane routing needs and completion_date cannot give: a data-only
    # change approved by the data group has to be recordable without waiting on
    # a business approval nobody asked for.
    reviews = [_review("danpelota", at="2026-08-05T02:07:22Z")]
    assert group_dates(reviews, DATA, BIZ) == {"data": "2026-08-05", "business": None}


def test_group_dates_uses_each_groups_earliest_approval():
    reviews = [
        _review("amanda847", at="2026-08-04T23:27:19Z"),
        _review("danpelota", at="2026-08-05T02:07:22Z"),
        _review("hhkarimi", at="2026-08-09T09:00:00Z"),
    ]
    assert group_dates(reviews, DATA, BIZ) == {"data": "2026-08-05", "business": "2026-08-04"}


def test_group_dates_ignores_bots():
    assert group_dates([_review("delegate-reviewer[bot]")], DATA, BIZ) == {"data": None, "business": None}


# --- which lane a change is in ----------------------------------------------


def test_a_wording_fix_asks_nobody():
    # #1046's shape. It printed an OKR as stale for four days and pulled both
    # groups onto a documentation change.
    before, after = [_rec("m", definition="old")], [_rec("m", definition="new")]
    classified = lanes.classify(before, after)
    assert classified[lanes.BUSINESS] == [] and classified[lanes.DATA] == []
    assert classified["unreviewed"] == ["m"]
    assert lanes.teams(classified) == []


def test_an_anchor_change_asks_the_data_group_only():
    # #1073's shape: which events count moved, so who gets counted moved. Under
    # the path rule this asked both groups; under one seal it flagged nothing.
    classified = lanes.classify([_rec("m")], [_rec("m", anchor="event=Replaced")])
    assert classified[lanes.DATA] == ["m"] and classified[lanes.BUSINESS] == []
    assert lanes.teams(classified) == ["semantic-layer-data"]


def test_a_rule_change_asks_the_business_group_only():
    classified = lanes.classify([_rec("m")], [_rec("m", rule="counts: somebody else")])
    assert classified[lanes.BUSINESS] == ["m"] and classified[lanes.DATA] == []
    assert lanes.teams(classified) == ["semantic-layer-business"]


def test_retiring_a_metric_is_a_business_call():
    # config.meta.retired ends the thing the business group ruled on. `era:
    # historical` on one leg retires one event and rides in anchored_on instead.
    classified = lanes.classify([_rec("m")], [_rec("m", retired="2026-08-01")])
    assert classified[lanes.BUSINESS] == ["m"]


def test_adding_or_removing_a_metric_asks_both():
    assert lanes.teams(lanes.classify([], [_rec("m")])) == ["semantic-layer-data", "semantic-layer-business"]
    assert lanes.teams(lanes.classify([_rec("m")], [])) == ["semantic-layer-data", "semantic-layer-business"]


def test_a_change_can_be_in_both_lanes():
    classified = lanes.classify(
        [_rec("m")], [_rec("m", rule="counts: somebody else", anchor="event=Replaced")]
    )
    assert classified[lanes.BUSINESS] == ["m"] and classified[lanes.DATA] == ["m"]


def test_an_owner_edit_asks_nobody():
    classified = lanes.classify([_rec("m", owner="team-a")], [_rec("m", owner="team-b")])
    assert lanes.teams(classified) == []


# --- what a merge earns ------------------------------------------------------


def test_new_metric_earns_both_halves():
    earned = ratifications.earned_by_merge([], [_rec("m")], BOTH, 800, VALUES)["m"]
    assert earned.rule.approved == "2026-08-07"
    assert earned.rule.sha == ratifications.rule_sha(_rec("m"))
    assert earned.data.sha == ratifications.build_sha(_rec("m"))
    assert earned.data.value == 100
    assert earned.approved_by_pr == 800


def test_a_data_only_approval_earns_the_build_half_alone():
    # The failure lane routing would otherwise create: under the single seal
    # this needed BOTH groups, so a correctly routed data-only change recorded
    # nothing and sat pending forever.
    before, after = [_rec("m")], [_rec("m", anchor="event=Replaced")]
    earned = ratifications.earned_by_merge(
        before, after, {"data": "2026-08-07", "business": None}, 800, VALUES
    )["m"]
    assert earned.data is not None and earned.rule is None


def test_a_build_half_with_no_declared_value_is_not_recorded():
    # The loader requires a value, so writing the entry anyway would produce a
    # sidecar that fails to load and take the blocking freshness gate with it.
    before, after = [_rec("m")], [_rec("m", anchor="event=Replaced")]
    assert ratifications.earned_by_merge(before, after, BOTH, 800, {}) == {}


def test_a_rule_change_needs_no_value():
    before, after = [_rec("m")], [_rec("m", rule="counts: somebody else")]
    earned = ratifications.earned_by_merge(before, after, BOTH, 800, {})["m"]
    assert earned.rule is not None and earned.data is None


def test_a_metric_with_no_business_rule_earns_no_rule_half():
    # Every ruleless metric seals identically over emptiness, so recording one
    # would assert an approval of nothing, for all of them at once.
    assert ratifications.earned_by_merge([], [_rec("m", rule=None)], BOTH, 800, VALUES)["m"].rule is None


def test_untouched_pending_metric_in_a_touched_file_earns_nothing():
    # The rule that stops one metric's PR from ratifying its file-mates.
    before = [_rec("edited", anchor="old"), _rec("bystander")]
    after = [_rec("edited", anchor="new"), _rec("bystander")]
    earned = ratifications.earned_by_merge(before, after, BOTH, 800, VALUES)
    assert set(earned) == {"edited"}


def test_an_already_signed_half_earns_nothing():
    before = [_rec("m", build_approved="2026-08-01")]
    after = [_rec("m", anchor="new", build_approved="2026-08-01")]
    assert ratifications.earned_by_merge(before, after, BOTH, 800, VALUES) == {}


def test_a_stale_half_is_re_earned_on_the_new_content():
    before = [_rec("m", anchor="old", build_approved="2026-08-01")]
    after = [_rec("m", anchor="new", build_approved="2026-08-01", build_stale=True)]
    earned = ratifications.earned_by_merge(before, after, BOTH, 800, VALUES)["m"]
    assert earned.data.approved == "2026-08-07"
    assert earned.data.sha == ratifications.build_sha(_rec("m", anchor="new"))


def test_retired_metric_earns_nothing():
    assert ratifications.earned_by_merge([], [_rec("m", retired="2026-08-01")], BOTH, 800, VALUES) == {}


def test_untouched_stale_metric_in_a_touched_file_earns_nothing():
    # Staleness can predate this merge entirely (an earlier hand-edit changed
    # the definition without a re-ratification). The file-mates rule applies
    # just as much to a stale bystander as to a pending one: no movement in
    # THIS merge means this PR's reviewers never saw it.
    bystander = dict(build_approved="2026-08-01", build_stale=True)
    before = [_rec("edited", anchor="old"), _rec("stale_bystander", **bystander)]
    after = [_rec("edited", anchor="new"), _rec("stale_bystander", **bystander)]
    earned = ratifications.earned_by_merge(before, after, BOTH, 800, VALUES)
    assert set(earned) == {"edited"}


def test_metadata_only_change_earns_nothing():
    # owner/detail_doc are in neither seal, so editing only owner is not "the
    # definition moved" -- nobody reviewed a changed definition.
    before, after = [_rec("m", owner="team-a")], [_rec("m", owner="team-b")]
    assert ratifications.earned_by_merge(before, after, BOTH, 800, VALUES) == {}


def test_a_wording_fix_earns_nothing():
    # The other half of what #1046 got wrong: rewording expired an approval AND
    # would have re-earned it off a review of prose.
    before, after = [_rec("m", definition="old")], [_rec("m", definition="new")]
    assert ratifications.earned_by_merge(before, after, BOTH, 800, VALUES) == {}


# --- writing it into the sidecar --------------------------------------------

EXISTING = """\
# Ratification sign-offs (header comment that must survive).

activated_serve_users:
  # Both groups approved #765, which contained the restatement.
  business:
    approved: 2026-08-05
    rule_sha: '5e87555'
  data:
    approved: 2026-08-05
    build_sha: '33429b6'
    value_at_signing: 1026
  approved_by_pr: 765

# win_activated_users is deliberately absent, so it reads pending.
# win_activated_users:
#   business:
"""


def _sign_off(date="2026-08-07", sha="abc1234", pr=800, rule=True, data=True, value=941):
    return ratifications.Ratification(
        rule=ratifications.SignOff(date, sha) if rule else None,
        data=ratifications.SignOff(date, sha, value) if data else None,
        approved_by_pr=pr,
    )


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
    assert loaded["win_users"].rule.approved == "2026-08-07"
    assert loaded["win_users"].data.value == 941
    assert loaded["activated_serve_users"].approved_by_pr == 765


def test_upsert_edits_an_existing_entry_in_place_without_duplicating_the_key(tmp_path):
    # The stale case: the metric already has a block, so appending a second one
    # would make a duplicate key that YAML silently resolves to the last.
    out = ratifications.upsert(EXISTING, "activated_serve_users", _sign_off(pr=900))
    assert out.count("activated_serve_users:") == 1
    assert "Both groups approved #765" in out, "the human's note must survive a re-date"
    assert _loaded(tmp_path, out)["activated_serve_users"].approved_by_pr == 900


def test_upsert_of_one_half_leaves_the_other_half_as_it_was(tmp_path):
    # Lane routing's consequence: a data-only merge must not restate or drop a
    # business approval given months earlier on a different PR.
    out = ratifications.upsert(EXISTING, "activated_serve_users", _sign_off(rule=False, pr=900))
    entry = _loaded(tmp_path, out)["activated_serve_users"]
    assert entry.rule.approved == "2026-08-05" and entry.rule.sha == "5e87555"
    assert entry.data.approved == "2026-08-07" and entry.data.value == 941


def test_upsert_editing_an_entry_preserves_an_unrelated_trailing_comment_block(tmp_path):
    # A regression in block-end detection would silently drop or reorder
    # someone else's sidecar comment. Compare byte for byte, not contains.
    trailing = EXISTING[EXISTING.index("\n# win_activated_users") :]
    out = ratifications.upsert(EXISTING, "activated_serve_users", _sign_off(pr=900))
    assert out.endswith(trailing)


def test_upsert_fills_in_a_block_that_has_only_comments(tmp_path):
    text = "m:\n  # hand-written note, no fields yet\n"
    out = ratifications.upsert(text, "m", _sign_off())
    assert _loaded(tmp_path, out)["m"].data.value == 941
    assert "# hand-written note, no fields yet" in out, "the human's comment survives"


def test_upsert_never_matches_a_commented_out_key(tmp_path):
    text = "# win_activated_users:\n#   business:\n"
    out = ratifications.upsert(text, "win_activated_users", _sign_off())
    assert out.startswith("# win_activated_users:"), "the commented block stays commented"
    assert _loaded(tmp_path, out)["win_activated_users"].approved_by_pr == 800


def test_upsert_writes_an_all_digit_hash_quoted(tmp_path):
    # Unquoted, YAML reads it as an integer and the leading zero is gone.
    out = ratifications.upsert(EXISTING, "win_users", _sign_off(sha="0123456"))
    assert _loaded(tmp_path, out)["win_users"].rule.sha == "0123456"


def test_upsert_into_an_empty_file_creates_a_single_loadable_entry(tmp_path):
    # The very first ratification ever: no sidecar content exists yet.
    loaded = _loaded(tmp_path, ratifications.upsert("", "m", _sign_off()))
    assert loaded["m"].rule.approved == "2026-08-07" and loaded["m"].data.value == 941


def test_upsert_with_a_note_adds_a_comment_that_survives_the_round_trip(tmp_path):
    # Comments are load-bearing: a note passed alongside a brand-new entry must
    # appear in the text, tagged with the auto-note prefix so recording.py can
    # find and replace its own notes later, and not break parsing.
    out = ratifications.upsert(EXISTING, "win_users", _sign_off(), note="Approved in #800.")
    assert f"# {ratifications.AUTO_NOTE_PREFIX}Approved in #800." in out
    assert _loaded(tmp_path, out)["win_users"].approved_by_pr == 800


def test_upsert_with_a_note_lands_on_an_edit(tmp_path):
    # A re-earned stale sign-off already has a block, so it takes the edit
    # path -- the note must not be silently dropped there.
    out = ratifications.upsert(
        EXISTING, "activated_serve_users", _sign_off(pr=200), note="Re-earned in #900."
    )
    assert f"# {ratifications.AUTO_NOTE_PREFIX}Re-earned in #900." in out
    assert _loaded(tmp_path, out)["activated_serve_users"].approved_by_pr == 200


def test_upsert_with_a_note_replaces_the_prior_auto_note_on_re_edit(tmp_path):
    # Re-recording must not accumulate a second auto-note each time a metric
    # goes stale and gets re-earned.
    once = ratifications.upsert(
        EXISTING, "activated_serve_users", _sign_off(pr=200), note="First re-earn, PR #900."
    )
    twice = ratifications.upsert(
        once, "activated_serve_users", _sign_off(pr=300), note="Second re-earn, PR #901."
    )
    assert twice.count(ratifications.AUTO_NOTE_PREFIX) == 1
    assert "First re-earn" not in twice
    assert f"# {ratifications.AUTO_NOTE_PREFIX}Second re-earn, PR #901." in twice
    assert _loaded(tmp_path, twice)["activated_serve_users"].approved_by_pr == 300


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
    # approved_by_pr is optional (a hand-authored entry may lack a PR). Writing
    # the bare word `None` would round-trip through load() as the STRING
    # "None", silently corrupting the type the rest of the code expects.
    out = ratifications.upsert("", "m", _sign_off(pr=None))
    assert _loaded(tmp_path, out)["m"].approved_by_pr is None


def test_upsert_is_idempotent_on_the_edit_path(tmp_path):
    once = ratifications.upsert(EXISTING, "activated_serve_users", _sign_off(pr=900))
    assert ratifications.upsert(once, "activated_serve_users", _sign_off(pr=900)) == once
