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


# --- orchestration (env + clients live only below this line) -----------------

import argparse  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import sys  # noqa: E402
import urllib.request  # noqa: E402
from pathlib import Path  # noqa: E402

from semantic_catalog import mentions as mentions_mod  # noqa: E402
from semantic_catalog import pr_marker, slack_diff, slack_reply  # noqa: E402
from semantic_catalog.github_client import GitHubClient  # noqa: E402
from semantic_catalog.parser import parse_semantic_tree  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_MODELS = REPO_ROOT / "dbt" / "project" / "models"
PKG = Path(__file__).parent
TEAM_SLUG = {"data": "semantic-layer-data", "business": "semantic-layer-business"}


def _github_client(token: str, repo: str) -> GitHubClient:
    # Forward a fresh `urllib.request.urlopen` lookup rather than relying on
    # GitHubClient's own default: that default is bound once at import time,
    # so a test-time monkeypatch of the module attribute never reaches it.
    return GitHubClient(token, repo, urlopen=urllib.request.urlopen)


def _changed_names(base_dir: Path | None) -> tuple[str, ...]:
    after = parse_semantic_tree([DBT_MODELS])
    before = parse_semantic_tree([base_dir / "dbt/project/models"]) if base_dir else []
    return tuple(slack_diff.changed_metric_names(before, after))


def _find_marker(gh: GitHubClient, pr: int) -> tuple[int, pr_marker.ThreadState] | None:
    for c in gh.issue_comments(pr):
        if pr_marker.is_marker(c.get("body", "")):
            state = pr_marker.parse(c["body"])
            if state is not None:
                return int(c["id"]), state
    return None


def _cmd_emit_marker(gh: GitHubClient, pr: int, out: Path) -> int:
    found = _find_marker(gh, pr)
    if found is None:
        out.write_text("{}")
        print("no marker found")
        return 0
    _, state = found
    out.write_text(json.dumps({"ts": state.ts, "merged": state.merged, "channel": state.channel}))
    print(f"marker: ts={state.ts} merged={state.merged}")
    return 0


def _cmd_mark_merged(gh: GitHubClient, pr: int) -> int:
    found = _find_marker(gh, pr)
    if found is None:
        print("no marker found; nothing to mark")
        return 0
    comment_id, state = found
    state.merged = True
    gh.update_comment(comment_id, pr_marker.render(state))
    print("marker marked merged")
    return 0


def _cmd_reconcile(event_path: Path, base_dir: Path | None) -> int:
    gh_token = os.environ.get("GH_TOKEN")
    slack_token = os.environ.get("SLACK_APP_BOT_TOKEN")
    channel = os.environ.get("SLACK_CHANNEL_ID")
    org_token = os.environ.get("ORG_READ_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    if not gh_token or not repo:
        print("GH_TOKEN/GITHUB_REPOSITORY not set; skipping thread reconcile.")
        return 0

    event = json.loads(event_path.read_text())
    pr = event["pull_request"]
    number = int(pr["number"])
    gh = _github_client(gh_token, repo)

    if not is_governed(gh.pr_files(number)):
        print(f"PR #{number} not governed; nothing to do.")
        return 0
    if not slack_token or not channel:
        print("SLACK_APP_BOT_TOKEN/SLACK_CHANNEL_ID not set; skipping thread reconcile.")
        return 0

    approvals: dict[str, list[str]] | None = None
    if org_token:
        gh_org = _github_client(org_token, repo)
        org = repo.split("/")[0]
        members = {team: gh_org.team_members(org, slug) for team, slug in TEAM_SLUG.items()}
        approvals = team_approvers(gh_org.pr_reviews(number), members)
    else:
        print("ORG_READ_TOKEN not set; skipping approval diff (lifecycle only).")

    found = _find_marker(gh, number)
    comment_id, state = found if found else (None, None)
    ctx = PRContext(
        number=number,
        title=pr["title"],
        url=pr["html_url"],
        draft=bool(pr.get("draft")),
        pr_state="closed" if pr.get("state") == "closed" else "open",
        merged=bool(pr.get("merged")),
        metric_names=_changed_names(base_dir) if state is None else (),
    )
    teams = mentions_mod.load(PKG / "config" / "slack_mentions.yml")
    mention_by_team = {t: mentions_mod.render_team(teams.get(t)) for t in ("data", "business")}

    plan = reconcile(state, approvals, ctx, mention_by_team)
    if plan.anchor_text is not None and plan.new_state is not None:
        ts = slack_reply.post_message(slack_token, channel, plan.anchor_text, urlopen=urllib.request.urlopen)
        plan.new_state.ts, plan.new_state.channel = ts, channel
        try:
            plan.new_state.permalink = slack_reply.get_permalink(
                slack_token, channel, ts, urlopen=urllib.request.urlopen
            )
        except RuntimeError as e:  # cosmetic only; the marker link degrades to blank
            print(f"permalink lookup failed: {e}")
    for text in plan.replies:
        slack_reply.post_message(
            slack_token,
            plan.new_state.channel or channel,
            text,
            thread_ts=plan.new_state.ts,
            urlopen=urllib.request.urlopen,
        )
    if plan.new_state is not None:
        body = pr_marker.render(plan.new_state)
        if comment_id is not None:
            gh.update_comment(comment_id, body)
        else:
            gh.create_comment(number, body)
    print(f"reconciled: anchor={'yes' if plan.anchor_text else 'no'} replies={len(plan.replies)}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="semantic_catalog.thread")
    ap.add_argument("--event-path", type=Path)
    ap.add_argument("--base-dir", type=Path, default=None)
    ap.add_argument("--emit-marker", type=Path)
    ap.add_argument("--mark-merged", action="store_true")
    ap.add_argument("--pr", type=int)
    args = ap.parse_args(argv)

    if args.emit_marker or args.mark_merged:
        gh_token = os.environ.get("GH_TOKEN")
        repo = os.environ.get("GITHUB_REPOSITORY", "")
        if not gh_token or not repo or args.pr is None:
            print("GH_TOKEN/GITHUB_REPOSITORY/--pr required; skipping.", file=sys.stderr)
            return 0
        gh = _github_client(gh_token, repo)
        if args.emit_marker:
            return _cmd_emit_marker(gh, args.pr, args.emit_marker)
        return _cmd_mark_merged(gh, args.pr)

    if args.event_path is None:
        ap.error("--event-path is required for reconcile mode")
    return _cmd_reconcile(args.event_path, args.base_dir)


if __name__ == "__main__":
    raise SystemExit(main())
