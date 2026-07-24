"""Generate the semantic-layer catalog artifacts from the dbt sem_*.yml files.

--check  : fail if the generated canonical_metrics.md region is stale (CI guard).
--write  : splice the region into both knowledge skills' canonical_metrics.md.
--emit-clickup / --emit-slack : write the rendered artifacts for the publish job.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from semantic_catalog import lifecycle as lc_mod
from semantic_catalog.clickup_page import render_page
from semantic_catalog.lifecycle import Lifecycle
from semantic_catalog.md_catalog import render_region, splice_region
from semantic_catalog.parser import parse_semantic_tree
from semantic_catalog.records import MetricRecord

# analytics/diagnostics/semantic_catalog/cli.py -> parents[0]=semantic_catalog,
# [1]=diagnostics, [2]=analytics, [3]=repo root.
REPO_ROOT = Path(__file__).resolve().parents[3]
DBT_MODELS = REPO_ROOT / "dbt" / "project" / "models" / "marts"
SEM_ROOTS = [DBT_MODELS / "analytics", DBT_MODELS / "civics"]
SKILLS_ROOT = REPO_ROOT / ".claude" / "skills"
MD_TARGETS = [
    SKILLS_ROOT / "win-analytics-knowledge" / "references" / "canonical_metrics.md",
    SKILLS_ROOT / "serve-analytics-knowledge" / "references" / "canonical_metrics.md",
]
PKG = Path(__file__).parent
OWNERS = yaml.safe_load((PKG / "config" / "owners.yml").read_text())
SOP_MD = (PKG / "templates" / "sop.md").read_text()


def region_is_current(target: Path, records: list[MetricRecord]) -> bool:
    if not target.exists():
        return False
    return splice_region(target.read_text(), render_region(records)) == target.read_text()


def write_region(target: Path, records: list[MetricRecord]) -> None:
    existing = target.read_text() if target.exists() else ""
    target.write_text(splice_region(existing, render_region(records)))


def _lifecycles(records: list[MetricRecord]) -> dict[str, Lifecycle]:
    files = {r.yaml_file for r in records}
    return {f: lc_mod.derive(f) for f in files}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="semantic_catalog")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--emit-clickup", type=Path)
    parser.add_argument("--emit-slack-page", type=Path)  # placeholder for the publish job
    args = parser.parse_args(argv)

    records = parse_semantic_tree(SEM_ROOTS)

    if args.check:
        stale = [t for t in MD_TARGETS if not region_is_current(t, records)]
        if stale:
            print("stale canonical_metrics.md region in:", file=sys.stderr)
            for t in stale:
                print(f"  {t}", file=sys.stderr)
            return 1
        print("catalog region up to date")
        return 0

    if args.write:
        for t in MD_TARGETS:
            write_region(t, records)
            print(f"wrote {t}")

    if args.emit_clickup:
        page = render_page(records, _lifecycles(records), SOP_MD, OWNERS)
        args.emit_clickup.write_text(page)
        print(f"wrote {args.emit_clickup}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
