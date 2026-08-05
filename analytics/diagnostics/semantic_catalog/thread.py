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
from pathlib import Path

from semantic_catalog.pr_marker import ThreadState

GOVERNED_RE = re.compile(r"^dbt/project/models/(?:.+/)?sem_[^/]+\.yml$")
TEAM_LABEL = {"data": "Data team", "business": "Business team"}


def is_governed(files: Iterable[str]) -> bool:
    return any(GOVERNED_RE.match(f) for f in files)


def governed_sem_files(files: Iterable[str]) -> frozenset[str]:
    """Basenames of the governed sem_*.yml files this PR touches.

    Bounds the anchor's metric list to definitions the PR actually owns, so a
    reviewer never has to guess which metric is up for review.
    """
    return frozenset(Path(f).name for f in files if GOVERNED_RE.match(f))


def team_approvers(reviews: list[dict], members: dict[str, list[str]]) -> dict[str, list[str]]:
    """Latest review per login; APPROVED only; bucketed by team, case-insensitive."""
    latest: dict[str, dict] = {}
    for r in reviews:
        # PENDING reviews come back from GET /pulls/{n}/reviews without a
        # submitted_at; they carry no verdict yet, so skip them.
        if not r.get("submitted_at"):
            continue
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
    # (name, one-line definition) per changed metric; definition may be "".
    metrics: tuple[tuple[str, str], ...] = ()


@dataclass
class Plan:
    anchor_text: str | None = None
    replies: list[str] = field(default_factory=list)
    new_state: ThreadState | None = None  # None => persist nothing


def render_anchor(ctx: PRContext, mention_by_team: dict[str, str]) -> str:
    # One bullet per metric with its one-line definition, matching the merge
    # message's "name — definition" copy so the thread reads consistently.
    if ctx.metrics:
        metric_lines = [f"• `{n}` — {d}" if d else f"• `{n}`" for n, d in ctx.metrics]
        metrics_block = "Metrics:\n" + "\n".join(metric_lines)
    else:
        metrics_block = "Metrics: (see PR diff)"
    reviewers = " · ".join(f"{team} {mention_by_team.get(team, '')}".strip() for team in ("data", "business"))
    return "\n".join(
        [
            f":scroll: Governed metric PR ready for review: *{ctx.title}*",
            ctx.url,
            metrics_block,
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
        if ctx.merged:
            # Merged PRs cannot reopen and nothing downstream needs
            # pr_state="closed" for them, so leave state untouched: this keeps
            # the re-rendered marker byte-identical to what's already posted,
            # which lets _cmd_reconcile skip the write entirely (fix for the
            # lost-update race against the publish workflow's mark-merged PATCH).
            pass
        else:
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


def _changed_metrics(
    base_dir: Path | None, scope: frozenset[str] | None = None
) -> tuple[tuple[str, str], ...]:
    """(name, one-line definition) per changed metric, for the anchor message.

    Definitions come from the post-change records, falling back to the
    pre-change ones for removed metrics.

    `scope` is the basenames of the sem files the PR touches. It is a hard
    bound: the anchor can only ever name metrics the PR actually owns. That
    matters because the base tree is best-effort (an unfetchable base sha
    leaves `base_dir` None), and without a before-set there is no diff to
    narrow by, so the fallback below would otherwise name the whole catalog.
    """
    after = parse_semantic_tree([DBT_MODELS])
    before = parse_semantic_tree([base_dir / "dbt/project/models"]) if base_dir else []
    definitions = {r.name: r.definition for r in before}
    definitions.update({r.name: r.definition for r in after})

    # With no before-set there is nothing to diff, so fall back to the whole
    # after-set and let `scope` below bound it to the PR's own files.
    names = slack_diff.changed_metric_names(before, after) if before else sorted({r.name for r in after})

    if scope is not None:
        owned = {r.name for r in (*before, *after) if Path(r.yaml_file).name in scope}
        names = [n for n in names if n in owned]
    return tuple((n, definitions.get(n, "")) for n in names)


# ts as rendered by pr_marker: a bare "<seconds>.<fraction>" Slack timestamp.
# Enforced before the value flows into GITHUB_ENV in the publish workflow, so a
# forged marker comment can't smuggle a newline (e.g. an env-var injection) in.
_TS_RE = re.compile(r"^\d+\.\d+$")
# Slack channel ids are uppercase alphanumeric. Same GITHUB_ENV-injection guard
# as _TS_RE: the marker's channel is exported as SLACK_CHANNEL_ID at merge time.
_CHANNEL_RE = re.compile(r"^[A-Z0-9]+$")


def _find_marker(gh: GitHubClient, pr: int) -> tuple[int, pr_marker.ThreadState, str] | None:
    for c in gh.issue_comments(pr):
        body = c.get("body", "")
        if pr_marker.is_marker(body):
            state = pr_marker.parse(body)
            if state is not None:
                return int(c["id"]), state, body
    return None


def _cmd_emit_marker(gh: GitHubClient, pr: int, out: Path) -> int:
    found = _find_marker(gh, pr)
    if found is None:
        out.write_text("{}")
        print("no marker found")
        return 0
    _, state, _ = found
    if not _TS_RE.match(state.ts):
        out.write_text("{}")
        print(f"warning: marker ts {state.ts!r} failed format validation; treating as no-marker")
        return 0
    if state.channel and not _CHANNEL_RE.match(state.channel):
        out.write_text("{}")
        print(f"warning: marker channel {state.channel!r} failed format validation; treating as no-marker")
        return 0
    out.write_text(json.dumps({"ts": state.ts, "merged": state.merged, "channel": state.channel}))
    print(f"marker: ts={state.ts} merged={state.merged}")
    return 0


def _cmd_mark_merged(gh: GitHubClient, pr: int) -> int:
    found = _find_marker(gh, pr)
    if found is None:
        # No governance thread existed for this PR (pre-thread PR, or a lost
        # marker). Bootstrap a merged marker from the merge post itself (its
        # ts/channel arrive via GITHUB_ENV from the publish workflow's post
        # step), so a publish re-run still sees merged:true and never
        # double-posts the merge summary.
        ts = os.environ.get("SLACK_TS", "")
        chan = os.environ.get("SLACK_CHANNEL_ID", "")
        if not _TS_RE.match(ts) or not _CHANNEL_RE.match(chan):
            print("no marker found and no valid SLACK_TS/SLACK_CHANNEL_ID; nothing to mark")
            return 0
        state = pr_marker.ThreadState(ts=ts, channel=chan, permalink="", merged=True)
        gh.create_comment(pr, pr_marker.render(state))
        print("no marker found; bootstrapped a merged marker from the merge post")
        return 0
    comment_id, state, _ = found
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

    try:
        pr_files = gh.pr_files(number)
        governed = is_governed(pr_files)
    except (OSError, RuntimeError) as e:
        # Same graceful degrade as every other external call here: a transient
        # GitHub error must not turn the (non-blocking) workflow step red.
        print(f"pr_files lookup failed ({e}); skipping thread reconcile.")
        return 0
    if not governed:
        print(f"PR #{number} not governed; nothing to do.")
        return 0
    if not slack_token or not channel:
        print("SLACK_APP_BOT_TOKEN/SLACK_CHANNEL_ID not set; skipping thread reconcile.")
        return 0

    approvals: dict[str, list[str]] | None = None
    if org_token:
        try:
            gh_org = _github_client(org_token, repo)
            org = repo.split("/")[0]
            members = {team: gh_org.team_members(org, slug) for team, slug in TEAM_SLUG.items()}
            # PR reviews belong to the workflow-token client (gh); org_token is
            # scoped to org team membership only (see github_client.py docstring).
            approvals = team_approvers(gh.pr_reviews(number), members)
        except (OSError, RuntimeError) as e:
            # Covers both the org-token team-membership lookup and the
            # workflow-token gh.pr_reviews call in this same try, so the
            # message stays token-neutral rather than blaming ORG_READ_TOKEN
            # for a failure that could originate from either call.
            print(f"approval lookup failed ({e}); skipping approval diff (lifecycle only).")
    else:
        print("ORG_READ_TOKEN not set; skipping approval diff (lifecycle only).")

    found = _find_marker(gh, number)
    comment_id, state, original_body = found if found else (None, None, None)
    ctx = PRContext(
        number=number,
        title=pr["title"],
        url=pr["html_url"],
        draft=bool(pr.get("draft")),
        pr_state="closed" if pr.get("state") == "closed" else "open",
        # pull_request_review payloads omit `merged` (only `merged_at` shows up
        # in some webhook shapes); check both so a review event on an already-
        # merged PR doesn't misread it as merged=False.
        merged=bool(pr.get("merged") or pr.get("merged_at")),
        metrics=(_changed_metrics(base_dir, governed_sem_files(pr_files)) if state is None else ()),
    )
    teams = mentions_mod.load(PKG / "config" / "slack_mentions.yml")
    mention_by_team = {t: mentions_mod.render_team(teams.get(t)) for t in ("data", "business")}

    plan = reconcile(state, approvals, ctx, mention_by_team)
    if plan.anchor_text is not None and plan.new_state is not None:
        try:
            ts = slack_reply.post_message(
                slack_token, channel, plan.anchor_text, urlopen=urllib.request.urlopen
            )
        except (RuntimeError, OSError) as e:
            # No anchor => nothing to thread under and no state worth writing.
            # Log and exit cleanly so the job stays green and the next event
            # retries the whole bootstrap.
            print(f"anchor post failed ({e!r}); aborting reconcile so the next event retries")
            return 0
        plan.new_state.ts, plan.new_state.channel = ts, channel
        try:
            plan.new_state.permalink = slack_reply.get_permalink(
                slack_token, channel, ts, urlopen=urllib.request.urlopen
            )
        except (RuntimeError, OSError) as e:  # cosmetic only; the marker link degrades to blank
            print(f"permalink lookup failed: {e}")
    for text in plan.replies:
        try:
            slack_reply.post_message(
                slack_token,
                plan.new_state.channel or channel,
                text,
                thread_ts=plan.new_state.ts,
                urlopen=urllib.request.urlopen,
            )
        except (RuntimeError, OSError) as e:
            # Non-fatal: the marker MUST still be written below, or the next
            # event finds no marker, re-bootstraps, and posts a duplicate
            # anchor (same failure class as the permalink lookup above).
            # Trade-off: this reply is lost (its transition is recorded as
            # announced), which beats a split thread.
            print(f"reply post failed ({e!r}); continuing so the marker is still written")
    if plan.new_state is not None:
        body = pr_marker.render(plan.new_state)
        if comment_id is not None:
            # Skip no-change writes: a re-render that matches the marker
            # already on the PR means nothing to persist, so don't PATCH it.
            # Every synchronize/review event otherwise re-wrote an unchanged
            # marker, widening the race window against the publish workflow's
            # mark-merged PATCH (last-writer-wins on the same comment).
            if body != original_body:
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
