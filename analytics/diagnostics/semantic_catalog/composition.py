"""Evaluate whether a PR's approvals cover both review groups.

Pure and side-effect free: the workflow supplies the approver logins and each
team's membership (from the GitHub API). Result feeds the merge-time Slack post.
The composition check is a signal, never a merge block (soft gate).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable


def _lower(names: Iterable[str]) -> set[str]:
    return {n.lower() for n in names}


def is_bot(login: str) -> bool:
    """GitHub appends [bot] to every App account's login."""
    return login.lower().endswith("[bot]")


def evaluate(
    approver_logins: Iterable[str],
    data_members: Iterable[str],
    business_members: Iterable[str],
) -> dict[str, bool]:
    # Bots are excluded explicitly, not merely by never being team members
    # (DATA-2249). delegate-reviewer[bot] approves nearly every governed PR, so
    # adding a bot to a review team for any reason would otherwise make the
    # two-group gate satisfiable with no human sign-off at all.
    approvers = _lower(login for login in approver_logins if not is_bot(login))
    return {
        "data": bool(approvers & _lower(data_members)),
        "business": bool(approvers & _lower(business_members)),
    }


def _latest_per_reviewer(reviews: list[dict]) -> list[dict]:
    """One review per login, the most recent, so a superseded verdict cannot count."""
    latest: dict[str, dict] = {}
    for review in reviews:
        login = review["login"]
        if login not in latest or review["submitted_at"] > latest[login]["submitted_at"]:
            latest[login] = review
    return list(latest.values())


def completed_at(
    reviews: list[dict],
    data_members: Iterable[str],
    business_members: Iterable[str],
) -> str | None:
    """When coverage completed: the moment the SECOND group's first approval landed.

    This is the date a ratification records. It is deliberately not "now" and not
    the date someone typed a line: `win_users` was recorded as 2026-08-03, the day
    it was authored, when coverage actually completed 2026-08-04.

    Returns an ISO-8601 UTC timestamp, or None if either group is uncovered.
    Each review is {"login", "state", "submitted_at"}.
    """
    approvals = [
        r for r in _latest_per_reviewer(reviews) if r["state"] == "APPROVED" and not is_bot(r["login"])
    ]
    first_per_group: list[str] = []
    for members in (data_members, business_members):
        lowered = _lower(members)
        stamps = sorted(r["submitted_at"] for r in approvals if r["login"].lower() in lowered)
        if not stamps:
            return None
        first_per_group.append(stamps[0])
    # Coverage is complete only once BOTH groups are in, so the later one wins.
    return max(first_per_group)


def completion_date(
    reviews: list[dict],
    data_members: Iterable[str],
    business_members: Iterable[str],
) -> str | None:
    """`completed_at` as the UTC calendar date, which is what the sidecar stores."""
    stamp = completed_at(reviews, data_members, business_members)
    return stamp[:10] if stamp else None


def _split_env(value: str | None) -> list[str]:
    if not value:
        return []
    return [x for x in value.split(",") if x]


def main(argv: list[str] | None = None) -> int:
    """Read APPROVERS/DATA/BIZ (comma-separated logins) from env and print coverage JSON."""
    del argv  # no CLI args; env-var driven for simple invocation from the workflow
    approvers = _split_env(os.environ.get("APPROVERS"))
    data_members = _split_env(os.environ.get("DATA"))
    business_members = _split_env(os.environ.get("BIZ"))
    coverage = evaluate(approvers, data_members, business_members)
    print(json.dumps(coverage))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
