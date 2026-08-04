from semantic_catalog.pr_marker import ThreadState
from semantic_catalog.thread import PRContext, is_governed, reconcile, render_anchor, team_approvers


def _ctx(**kw):
    base = dict(
        number=7,
        title="Ratify win_users",
        url="https://github.com/x/pull/7",
        draft=False,
        pr_state="open",
        merged=False,
        metric_names=("win_users",),
    )
    base.update(kw)
    return PRContext(**base)


def _state(**kw):
    base = dict(ts="1722.0001", channel="C1", permalink="https://p")
    base.update(kw)
    return ThreadState(**base)


NO_APPROVALS = {"data": [], "business": []}
MENTIONS = {"data": "<@U1>", "business": "<@U2> <@U3>"}


# --- is_governed -------------------------------------------------------------


def test_is_governed_matches_nested_and_direct_sem_files():
    assert is_governed(["dbt/project/models/marts/sem_analytics__users_win.yml"])
    assert is_governed(["dbt/project/models/sem_x.yml", "README.md"])
    assert not is_governed(["dbt/project/models/marts/users_win.yml", "analytics/foo.py"])
    assert not is_governed([])


# --- team_approvers ----------------------------------------------------------


def test_team_approvers_latest_review_wins_and_buckets_by_team():
    reviews = [
        {"user": {"login": "Alice"}, "state": "APPROVED", "submitted_at": "2026-08-01T10:00:00Z"},
        {"user": {"login": "alice"}, "state": "CHANGES_REQUESTED", "submitted_at": "2026-08-02T10:00:00Z"},
        {"user": {"login": "joe"}, "state": "APPROVED", "submitted_at": "2026-08-02T11:00:00Z"},
        {"user": {"login": "rando"}, "state": "APPROVED", "submitted_at": "2026-08-02T12:00:00Z"},
    ]
    members = {"data": ["alice"], "business": ["Joe", "amanda"]}
    out = team_approvers(reviews, members)
    assert out == {"data": [], "business": ["joe"]}  # alice superseded; rando in no team


# --- reconcile ---------------------------------------------------------------


def test_draft_is_silent():
    plan = reconcile(None, NO_APPROVALS, _ctx(draft=True), MENTIONS)
    assert plan.anchor_text is None and plan.replies == [] and plan.new_state is None


def test_bootstrap_anchor_on_open_pr_without_state():
    plan = reconcile(None, NO_APPROVALS, _ctx(), MENTIONS)
    assert plan.anchor_text is not None
    assert "`win_users`" in plan.anchor_text and "<@U1>" in plan.anchor_text
    assert plan.new_state is not None and plan.new_state.announced == {"data": None, "business": None}


def test_no_bootstrap_for_closed_pr_without_state():
    plan = reconcile(None, NO_APPROVALS, _ctx(pr_state="closed"), MENTIONS)
    assert plan.anchor_text is None and plan.replies == [] and plan.new_state is None


def test_approval_posts_once_then_replays_are_silent():
    st = _state()
    plan = reconcile(st, {"data": ["alice"], "business": []}, _ctx(), MENTIONS)
    assert plan.replies == [":white_check_mark: Data team approved (alice)"]
    assert plan.new_state.announced["data"] == "approved"
    replay = reconcile(plan.new_state, {"data": ["alice"], "business": []}, _ctx(), MENTIONS)
    assert replay.replies == []


def test_dismissal_and_reapproval_both_post():
    st = _state(announced={"data": "approved", "business": None})
    plan = reconcile(st, NO_APPROVALS, _ctx(), MENTIONS)
    assert plan.replies == [":leftwards_arrow_with_hook: Data team approval dismissed, re-review needed"]
    assert plan.new_state.announced["data"] == "dismissed"
    again = reconcile(plan.new_state, {"data": ["alice"], "business": []}, _ctx(), MENTIONS)
    assert again.replies == [":white_check_mark: Data team approved (alice)"]


def test_approvals_none_skips_approval_diff():
    st = _state(announced={"data": "approved", "business": None})
    plan = reconcile(st, None, _ctx(), MENTIONS)
    assert plan.replies == []  # no spurious dismissal when ORG_READ_TOKEN is missing
    assert plan.new_state.announced["data"] == "approved"


def test_close_without_merge_posts_and_close_with_merge_is_silent():
    plan = reconcile(_state(), NO_APPROVALS, _ctx(pr_state="closed", merged=False), MENTIONS)
    assert ":x: Closed without merging." in plan.replies
    assert plan.new_state.pr_state == "closed"
    merged = reconcile(_state(), NO_APPROVALS, _ctx(pr_state="closed", merged=True), MENTIONS)
    assert merged.replies == [] and merged.new_state.pr_state == "closed"


def test_reopen_posts_and_rerun_is_silent():
    st = _state(pr_state="closed")
    plan = reconcile(st, NO_APPROVALS, _ctx(pr_state="open"), MENTIONS)
    assert ":arrows_counterclockwise: Reopened." in plan.replies
    rerun = reconcile(plan.new_state, NO_APPROVALS, _ctx(pr_state="open"), MENTIONS)
    assert rerun.replies == []


def test_bootstrap_and_approval_in_same_event():
    plan = reconcile(None, {"data": ["alice"], "business": []}, _ctx(), MENTIONS)
    assert plan.anchor_text is not None
    assert plan.replies == [":white_check_mark: Data team approved (alice)"]


def test_render_anchor_without_mentions_or_names():
    text = render_anchor(_ctx(metric_names=()), {"data": "", "business": ""})
    assert "(see PR diff)" in text and "Reviewers" in text
