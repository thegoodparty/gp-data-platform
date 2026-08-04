"""Reconcile the Slack governance thread for a governed metric PR (DATA-2218).

Pure state machine: given the persisted ThreadState (or None), the current
approval picture, and the PR context, decide which Slack posts to make and the
new state to persist. No network in this half; orchestration (main) wires the
GitHub/Slack clients and lives in the same module to keep the entry point
discoverable as `python -m semantic_catalog.thread`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field

from semantic_catalog.pr_marker import ThreadState

GOVERNED_RE = re.compile(r"^dbt/project/models/(?:.+/)?sem_[^/]+\.yml$")
TEAM_LABEL = {"data": "Data team", "business": "Business team"}


def is_governed(files: Iterable[str]) -> bool:
    return any(GOVERNED_RE.match(f) for f in files)


def team_approvers(reviews: list[dict], members: dict[str, list[str]]) -> dict[str, list[str]]:
    """Latest review per login; APPROVED only; bucketed by team, case-insensitive."""
    latest: dict[str, dict] = {}
    for r in reviews:
        login = r["user"]["login"].lower()
        if login not in latest or r["submitted_at"] >= latest[login]["submitted_at"]:
            latest[login] = r
    approvers = {login for login, r in latest.items() if r["state"] == "APPROVED"}
    return {
        team: sorted(approvers & {m.lower() for m in team_members}) for team, team_members in members.items()
    }


@dataclass(frozen=True)
class PRContext:
    number: int
    title: str
    url: str
    draft: bool
    pr_state: str  # "open" | "closed"
    merged: bool
    metric_names: tuple[str, ...] = ()


@dataclass
class Plan:
    anchor_text: str | None = None
    replies: list[str] = field(default_factory=list)
    new_state: ThreadState | None = None  # None => persist nothing


def render_anchor(ctx: PRContext, mention_by_team: dict[str, str]) -> str:
    names = ", ".join(f"`{n}`" for n in ctx.metric_names) or "(see PR diff)"
    reviewers = " · ".join(f"{team} {mention_by_team.get(team, '')}".strip() for team in ("data", "business"))
    return "\n".join(
        [
            f":scroll: Governed metric PR ready for review: *{ctx.title}*",
            ctx.url,
            f"Metrics: {names}",
            f"Reviewers: {reviewers}",
            "Approvals will be tracked in this thread.",
        ]
    )


def reconcile(
    state: ThreadState | None,
    approvals: dict[str, list[str]] | None,
    ctx: PRContext,
    mention_by_team: dict[str, str],
) -> Plan:
    """One diff-based pass; every trigger event takes this same path.

    approvals=None means the org token was unavailable: skip the approval diff
    entirely (never dismiss on missing data). Lifecycle diffs still run.
    """
    plan = Plan()
    if ctx.draft:
        return plan
    if state is None:
        if ctx.pr_state != "open":
            return plan  # never bootstrap a thread just to announce a close
        plan.anchor_text = render_anchor(ctx, mention_by_team)
        state = ThreadState(
            ts="", channel="", permalink=""
        )  # ts/channel/permalink filled by caller after posting

    if approvals is not None:
        for team in ("data", "business"):
            approved = bool(approvals.get(team))
            announced = state.announced.get(team)
            if approved and announced != "approved":
                who = ", ".join(approvals[team])
                plan.replies.append(f":white_check_mark: {TEAM_LABEL[team]} approved ({who})")
                state.announced[team] = "approved"
            elif not approved and announced == "approved":
                plan.replies.append(
                    f":leftwards_arrow_with_hook: {TEAM_LABEL[team]} approval dismissed, re-review needed"
                )
                state.announced[team] = "dismissed"

    if ctx.pr_state == "closed" and state.pr_state == "open":
        if not ctx.merged:
            plan.replies.append(":x: Closed without merging.")
        state.pr_state = "closed"
    elif ctx.pr_state == "open" and state.pr_state == "closed":
        plan.replies.append(":arrows_counterclockwise: Reopened.")
        state.pr_state = "open"

    plan.new_state = state
    return plan
