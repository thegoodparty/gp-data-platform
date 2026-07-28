"""Guard: every semantic definition must live in a governed sem_*.yml file.

The whole governance gate keys off one filename convention: sem_*.yml under
dbt/project/models. CODEOWNERS auto-requests the reviewer groups on it, the
composition and catalog-freshness checks trigger on it, and the catalog parser
reads it. A semantic_models:/metrics: block in a differently-named file, or in a
sem_*.yaml (wrong extension), would slip past every one of those at once:
reviewers never requested, never catalogued, never announced on merge.

This check fails when it finds such a file, turning the naming convention into
an enforced invariant so a governed metric can never escape coverage.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

# analytics/diagnostics/semantic_catalog/naming_guard.py -> parents[3] = repo root.
REPO_ROOT = Path(__file__).resolve().parents[3]
MODELS_ROOT = REPO_ROOT / "dbt" / "project" / "models"


def _defines_semantics(path: Path) -> bool:
    """True if the YAML has a top-level semantic_models: or metrics: block."""
    try:
        doc = yaml.safe_load(path.read_text())
    except yaml.YAMLError:
        # Malformed YAML is check-yaml's job, not ours; do not crash here.
        return False
    return isinstance(doc, dict) and bool(doc.get("semantic_models") or doc.get("metrics"))


def _is_governed_name(path: Path) -> bool:
    """The exact pattern the CODEOWNERS filter and the parser both key on."""
    return path.name.startswith("sem_") and path.suffix == ".yml"


def find_misplaced(models_root: Path = MODELS_ROOT) -> list[Path]:
    """Semantic files that escape the sem_*.yml convention, sorted by path."""
    misplaced: list[Path] = []
    for path in sorted(models_root.rglob("*")):
        if path.is_dir() or path.suffix not in (".yml", ".yaml"):
            continue
        if _is_governed_name(path):
            continue
        if _defines_semantics(path):
            misplaced.append(path)
    return misplaced


def main() -> int:
    misplaced = find_misplaced()
    if misplaced:
        print(
            "Semantic definitions (semantic_models:/metrics:) must live in a governed "
            "sem_*.yml file so the review gate and catalog cover them. These files "
            "escape the convention and would bypass governance entirely:",
            file=sys.stderr,
        )
        for path in misplaced:
            print(f"  {path.relative_to(REPO_ROOT)}", file=sys.stderr)
        print(
            "Rename each to sem_<name>.yml (prefix sem_, extension .yml).",
            file=sys.stderr,
        )
        return 1
    print("all semantic definitions live in governed sem_*.yml files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
