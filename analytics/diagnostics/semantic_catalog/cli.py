"""Generate the semantic-layer catalog artifacts from the dbt sem_*.yml files.

--check  : fail if the generated canonical_metrics.md region is stale (CI guard).
--write  : splice the region into both knowledge skills' canonical_metrics.md.
--emit-clickup / --emit-slack : write the rendered artifacts for the publish job.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

from semantic_catalog import lifecycle as lc_mod
from semantic_catalog.clickup_page import render_page
from semantic_catalog.lifecycle import Lifecycle
from semantic_catalog.md_catalog import render_region, splice_region
from semantic_catalog.parser import parse_semantic_tree
from semantic_catalog.records import MetricRecord
from semantic_catalog.slack_diff import render_message

# analytics/diagnostics/semantic_catalog/cli.py -> parents[0]=semantic_catalog,
# [1]=diagnostics, [2]=analytics, [3]=repo root.
REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_MODELS = REPO_ROOT / "dbt" / "project" / "models" / "marts"
SEM_ROOTS = [DBT_MODELS / "analytics", DBT_MODELS / "civics"]
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


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="semantic_catalog")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--emit-clickup", type=Path)
    parser.add_argument("--emit-slack", type=Path)
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
        args.emit_clickup.write_text(page)
        print(f"wrote {args.emit_clickup}")

    if args.emit_slack:
        after = records
        before = (
            parse_semantic_tree(
                [
                    args.base_dir / "dbt/project/models/marts/analytics",
                    args.base_dir / "dbt/project/models/marts/civics",
                ]
            )
            if args.base_dir
            else []
        )
        coverage = json.loads(args.coverage) if args.coverage else {"data": False, "business": False}
        msg = render_message(before, after, args.pr_url, coverage)
        args.emit_slack.write_text(msg)
        print(f"wrote {args.emit_slack}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
