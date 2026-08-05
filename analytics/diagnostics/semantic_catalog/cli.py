"""Generate the semantic-layer catalog artifacts from the dbt sem_*.yml files.

--check  : fail if the generated canonical_metrics.md region is stale (CI guard).
--write  : splice the region into both knowledge skills' canonical_metrics.md.
--emit-clickup / --emit-slack : write the rendered artifacts for the publish job.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import yaml

from semantic_catalog import lifecycle as lc_mod
from semantic_catalog.clickup_page import CATALOG_BEGIN, CATALOG_END, render_page
from semantic_catalog import sigma_tasks
from semantic_catalog import sigma_tasks, slack_reply
from semantic_catalog.clickup_client import ClickUpClient
from semantic_catalog.lifecycle import Lifecycle
from semantic_catalog.md_catalog import render_region, splice_region
from semantic_catalog.parser import parse_semantic_tree
from semantic_catalog.records import MetricRecord
from semantic_catalog.slack_diff import render_message

# analytics/diagnostics/semantic_catalog/cli.py -> parents[0]=semantic_catalog,
# [1]=diagnostics, [2]=analytics, [3]=repo root.
REPO_ROOT = Path(__file__).resolve().parents[3]
# Parse EVERY sem_*.yml under models, not an allow-list of subdirs, so the
# parser's coverage matches the CODEOWNERS glob (/dbt/project/models/**/sem_*.yml)
# exactly. A governed file added under any subdir is auto-catalogued; the naming
# guard (semantic_catalog.naming_guard) enforces that all semantic content lives
# in a sem_*.yml so nothing can escape this scope.
DBT_MODELS = REPO_ROOT / "dbt" / "project" / "models"
SEM_ROOTS = [DBT_MODELS]
SKILLS_ROOT = REPO_ROOT / ".claude" / "skills"

# Each skill owns one canonical_metrics.md. The generated region is projected
# PER SKILL so a product's cheat sheet lists only its own metrics, and each
# row's detail_doc link resolves inside that skill's own references/ dir.
MD_TARGET_BY_SKILL = {
    "win-analytics-knowledge": SKILLS_ROOT
    / "win-analytics-knowledge"
    / "references"
    / "canonical_metrics.md",
    "serve-analytics-knowledge": SKILLS_ROOT
    / "serve-analytics-knowledge"
    / "references"
    / "canonical_metrics.md",
}

# Which skill each governed sem_*.yml routes its metrics to. Civics outcome
# metrics are documented in the Win skill (outcomes.md lives there), so they
# route to win rather than to a civics-specific sheet. A sem file absent from
# this map is a hard error (see records_by_target) so a new metric can never
# silently land in the wrong sheet, or none at all.
SKILL_BY_SEM_FILE = {
    "sem_analytics__users_win.yml": "win-analytics-knowledge",
    "sem_analytics__users_serve.yml": "serve-analytics-knowledge",
    "sem_civics__candidacy_stage.yml": "win-analytics-knowledge",
}
PKG = Path(__file__).parent


def region_is_current(target: Path, records: list[MetricRecord]) -> bool:
    if not target.exists():
        return False
    try:
        return splice_region(target.read_text(), render_region(records)) == target.read_text()
    except ValueError:
        # Half-marked/corrupt file (one marker present): report as not current
        # so --check fails cleanly as stale, not with an uncaught traceback.
        return False


def write_region(target: Path, records: list[MetricRecord]) -> None:
    existing = target.read_text() if target.exists() else ""
    target.write_text(splice_region(existing, render_region(records)))


def records_by_target(records: list[MetricRecord]) -> dict[Path, list[MetricRecord]]:
    """Group records by the canonical_metrics.md they should render into.

    Routes each record via its source sem file (SKILL_BY_SEM_FILE). Raises on an
    unmapped sem file so a newly-added metric fails loudly rather than being
    dropped from every sheet. Every known target appears in the result, with an
    empty list when a skill currently has no metrics.
    """
    grouped: dict[Path, list[MetricRecord]] = {t: [] for t in MD_TARGET_BY_SKILL.values()}
    for rec in records:
        sem_file = Path(rec.yaml_file).name
        skill = SKILL_BY_SEM_FILE.get(sem_file)
        if skill is None:
            raise ValueError(
                f"{sem_file} has no route in SKILL_BY_SEM_FILE; add a mapping so its "
                "metrics land in a canonical_metrics.md."
            )
        grouped[MD_TARGET_BY_SKILL[skill]].append(rec)
    return grouped


def _lifecycles(records: list[MetricRecord]) -> dict[str, Lifecycle]:
    files = {r.yaml_file for r in records}
    return {f: lc_mod.derive(f) for f in files}


def _before_after(base_dir: Path | None) -> tuple[list[MetricRecord], list[MetricRecord]]:
    """Records as of HEAD (after) and as of the merge base (before, empty if none)."""
    after = parse_semantic_tree(SEM_ROOTS)
    before = parse_semantic_tree([base_dir / "dbt/project/models"]) if base_dir else []
    return before, after


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="semantic_catalog")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--emit-clickup", type=Path)
    parser.add_argument("--emit-slack", type=Path)
    parser.add_argument("--sync-sigma-tasks", action="store_true")
    parser.add_argument("--emit-created", type=Path)
    parser.add_argument("--reply-created", type=Path)
    parser.add_argument("--base-dir", type=Path, default=None)
    parser.add_argument("--pr-url", type=str, default="")
    parser.add_argument("--coverage", type=str, default="")
    args = parser.parse_args(argv)

    records = parse_semantic_tree(SEM_ROOTS)

    if args.check:
        grouped = records_by_target(records)
        stale = [t for t, recs in grouped.items() if not region_is_current(t, recs)]
        if stale:
            print("stale canonical_metrics.md region in:", file=sys.stderr)
            for t in stale:
                print(f"  {t}", file=sys.stderr)
            return 1
        print("catalog region up to date")
        return 0

    if args.write:
        for t, recs in records_by_target(records).items():
            write_region(t, recs)
            print(f"wrote {t}")

    if args.emit_clickup:
        owners = yaml.safe_load((PKG / "config" / "owners.yml").read_text())
        sop_md = (PKG / "templates" / "sop.md").read_text()
        footer_md = (PKG / "templates" / "footer.md").read_text()
        page = render_page(records, _lifecycles(records), sop_md, owners, footer_md=footer_md)
        # The catalog markers exist for splice-based updates; the ClickUp publish
        # is a full-page replace, and ClickUp's markdown parser glues a trailing
        # HTML comment onto the next heading. Drop the markers from the emitted page.
        page = (
            "\n".join(ln for ln in page.splitlines() if ln.strip() not in (CATALOG_BEGIN, CATALOG_END)) + "\n"
        )
        args.emit_clickup.write_text(page)
        print(f"wrote {args.emit_clickup}")

    if args.emit_slack:
        before, after = _before_after(args.base_dir)
        coverage = json.loads(args.coverage) if args.coverage else {"data": False, "business": False}
        msg = render_message(before, after, args.pr_url, coverage)
        args.emit_slack.write_text(msg)
        print(f"wrote {args.emit_slack}")

    if args.sync_sigma_tasks:
        token = os.environ.get("CLICKUP_TASK_TOKEN")
        if not token:
            print("CLICKUP_TASK_TOKEN not set; skipping Sigma build-task creation.")
            return 0
        cfg = yaml.safe_load((PKG / "config" / "sigma_tasks.yml").read_text())
        assignee_id = cfg.get("default_assignee_id")
        assignee_ids = (int(assignee_id),) if assignee_id else ()
        # No base dir (e.g. zero-sha before) => before is empty, so all currently-ratified metrics look new; ClickUp dedupe absorbs this. Matches the Slack step.
        before, after = _before_after(args.base_dir)
        client = ClickUpClient(token)
        result = sigma_tasks.sync(
            client, cfg["list_id"], cfg["build_key_field_id"], before, after, assignee_ids=assignee_ids
        )
        created_names = [c.metric_name for c in result.created]
        print(f"created {len(created_names)}: {', '.join(created_names) or '(none)'}")
        print(f"skipped {len(result.skipped)}: {', '.join(result.skipped) or '(none)'}")
        if args.emit_created:
            # The workflow reads this to post one threaded Slack reply per created task.
            args.emit_created.write_text(
                json.dumps(
                    [{"metric": c.metric_name, "task_id": c.task_id, "url": c.url} for c in result.created]
                )
            )
        return 0

    if args.reply_created:
        # Secrets stay in the environment, never on the command line.
        token = os.environ.get("SLACK_APP_BOT_TOKEN")
        thread_ts = os.environ.get("SLACK_TS")
        channel = os.environ.get("SLACK_CHANNEL_ID")
        if not token or not thread_ts or not channel:
            print("SLACK_APP_BOT_TOKEN/SLACK_TS/SLACK_CHANNEL_ID not all set; skipping thread replies.")
            return 0
        if not args.reply_created.exists():
            print(f"{args.reply_created} not found; skipping thread replies.")
            return 0
        tasks = json.loads(args.reply_created.read_text())
        slack_reply.reply_in_thread(token, channel, thread_ts, tasks)
        print(f"posted {len(tasks)} thread repl{'y' if len(tasks) == 1 else 'ies'}")
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
