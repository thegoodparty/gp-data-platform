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


def evaluate(
    approver_logins: Iterable[str],
    data_members: Iterable[str],
    business_members: Iterable[str],
) -> dict[str, bool]:
    approvers = _lower(approver_logins)
    return {
        "data": bool(approvers & _lower(data_members)),
        "business": bool(approvers & _lower(business_members)),
    }


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
