"""Git-derived lifecycle dates for a semantic YAML file.

created/last_updated (and their PR numbers) come from git history, so they can
never fall out of date and are never hand-authored. Granularity is per-FILE, not
per-metric: a sem_*.yml holds a small set of related metrics, and per-block
line-range attribution is a later refinement. PR numbers are parsed from
`Merge pull request #N` subjects; squash-merge repos can also match `(#N)`.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass

_PR_RE = re.compile(r"(?:Merge pull request #|\(#)(\d+)")
_SEP = "\x1f"


@dataclass(frozen=True)
class Lifecycle:
    created: str | None
    created_pr: str | None
    last_updated: str | None
    last_updated_pr: str | None


def _default_run_git(args: list[str]) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout.strip()


def _pr(subject: str) -> str | None:
    match = _PR_RE.search(subject)
    return match.group(1) if match else None


def derive(yaml_file: str, run_git=_default_run_git) -> Lifecycle:
    try:
        out = run_git(["log", "--follow", f"--format=%ad{_SEP}%s", "--date=short", "--", yaml_file])
    except (subprocess.SubprocessError, OSError):
        # git missing, shallow clone where --follow is unavailable, corrupt
        # repo, etc.: degrade to an empty lifecycle rather than crashing the
        # generator (same graceful-degradation as the no-history case below).
        return Lifecycle(None, None, None, None)
    lines = [ln for ln in out.splitlines() if ln.strip()]
    if not lines:
        return Lifecycle(None, None, None, None)
    newest_date, _, newest_subject = lines[0].partition(_SEP)
    oldest_date, _, oldest_subject = lines[-1].partition(_SEP)
    return Lifecycle(
        created=oldest_date,
        created_pr=_pr(oldest_subject),
        last_updated=newest_date,
        last_updated_pr=_pr(newest_subject),
    )
