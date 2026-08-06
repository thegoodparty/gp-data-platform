"""GitHub-team -> Slack mention rendering for the governance thread anchor.

Config is a non-secret YAML next to sigma_tasks.yml. Empty or missing config
degrades to no mentions; a missing courtesy ping must never fail the thread.
"""

from __future__ import annotations

from pathlib import Path

import yaml


def load(path: Path) -> dict:
    """Return the teams mapping from slack_mentions.yml ({} when absent/empty)."""
    if not path.exists():
        return {}
    doc = yaml.safe_load(path.read_text()) or {}
    return doc.get("teams") or {}


def render_team(team_cfg: dict | None) -> str:
    """Render one team's mention string: usergroup wins, then member ids, then nothing."""
    if not team_cfg:
        return ""
    usergroup = team_cfg.get("usergroup_id")
    if usergroup:
        return f"<!subteam^{usergroup}>"
    return " ".join(f"<@{m}>" for m in team_cfg.get("member_ids") or [])
