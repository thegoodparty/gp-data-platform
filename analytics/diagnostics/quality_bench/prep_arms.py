# analytics/diagnostics/quality_bench/prep_arms.py
"""Build the three arm environments additively (DATA-2164 pre-registration
amendment; original subtractive design was §6).

One-factor construction: every arm = the same substrate plus zero or more
treatment layers. Nothing is built by deletion, so treatment content can only
be present in an arm because a layer explicitly put it there, and
manifest.json records which layer contributed every file.

Contamination rules:
- Treatment layers are per-path `git archive <ref> -- <paths>` exports: no
  .git, no history. quality_bench/ (keys, questions, harness) is not in any
  layer, so it is absent from every arm by construction.
- Arms live at fresh paths, so project-keyed auto-memory does not attach.
- The substrate is identical across arms (same floor CLAUDE.md, same
  analytics/ scaffold, same settings), so bare vs knowledge vs full differ
  ONLY by treatment layers.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import subprocess
import tarfile
from pathlib import Path

try:
    from quality_bench import integrity
    from quality_bench.bank import ARMS
except ImportError:  # bare `python prep_arms.py`: diagnostics/ isn't on sys.path yet
    import sys

    sys.path.insert(0, str(Path(__file__).parents[1]))
    from quality_bench import integrity
    from quality_bench.bank import ARMS

# Treatment layers: repo-relative paths exported verbatim from --ref.
LAYERS: dict[str, list[str]] = {
    "knowledge": [
        ".claude/skills/win-analytics-knowledge",
        ".claude/skills/serve-analytics-knowledge",
        "analytics/lib/win_analysis.py",
        "analytics/lib/serve_analysis.py",
    ],
    "process": [
        ".claude/skills/analytics-process",
        ".claude/agents/product-data-scientist.md",
        ".claude/agents/product-manager.md",
    ],
}
# code-critic (repo review) and data-matching are deliberately NOT layers:
# they are not part of the pre-registered treatment.
ARM_LAYERS: dict[str, list[str]] = {
    "bare": [],
    "knowledge": ["knowledge"],
    "full": ["knowledge", "process"],
}
SUBSTRATE_EXPORTS = ["analytics/lib/databricks_conn.py"]

SETTINGS_JSON = {
    "permissions": {
        "defaultMode": "acceptEdits",
        "allow": [
            "Bash(*)",
            "Read(*)",
            "Write(*)",
            "Edit(*)",
            "Glob(*)",
            "Grep(*)",
            "Skill",
            "Task",
            "Agent",
            "ToolSearch",
            "TodoWrite",
        ],
        "deny": ["WebFetch", "WebSearch"],
    }
}

# Keep version pins in step with analytics/pyproject.toml: the arms run the
# exported analysis libs (win/serve_analysis), which are developed and tested
# against the analytics env's resolution — notably pandas <3.
ARM_PYPROJECT = """\
[project]
name = "bench-env"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = ["pandas>=2.3.1,<3.0.0", "databricks-sql-connector>=3", "databricks-sdk>=0.20"]

[tool.uv]
package = false
"""


def _fill(floor_text: str) -> str:
    """All arms share the analytics/ scaffold, so the floor placeholders fill
    identically everywhere (this used to differ between bare and the rest)."""
    return floor_text.replace("{{LIB_PATH}}", "analytics/lib").replace("{{UV_PROJECT}}", "analytics")


def _write_settings(arm_dir: Path) -> None:
    d = arm_dir / ".claude"
    d.mkdir(parents=True, exist_ok=True)
    (d / "settings.local.json").write_text(json.dumps(SETTINGS_JSON, indent=2))


def _export_paths(repo_root: Path, dest: Path, ref: str, paths: list[str]) -> list[str]:
    """git-archive `paths` at `ref` into `dest`; return the extracted file
    paths (dest-relative). Errors loudly if any path is missing at ref, so a
    renamed skill cannot silently produce a thinner arm."""
    proc = subprocess.run(
        ["git", "archive", ref, "--", *paths],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git archive failed for {ref} -- {paths} in {repo_root}: {stderr}")
    with tarfile.open(fileobj=io.BytesIO(proc.stdout)) as tar:
        names = [m.name for m in tar.getmembers() if m.isfile()]
        tar.extractall(dest, filter="data")
    _verify_export_coverage(paths, names, ref)
    return names


def _verify_export_coverage(paths: list[str], names: list[str], ref: str) -> None:
    """Every requested path must have contributed at least one extracted file.

    git archive hard-fails on a fully-unmatched pathspec, but this invariant
    is git-version behavior, not ours — encode it locally so arm construction
    never depends on it. A path is covered when the archive contains the path
    itself (a file) or something under it (a directory)."""
    uncovered = [p for p in paths if not any(n == p or n.startswith(p + "/") for n in names)]
    if uncovered:
        raise RuntimeError(
            f"git archive at {ref} produced no files for {uncovered}; "
            "renamed or emptied layer paths must fail prep, not thin the arm"
        )


def build_substrate(repo_root: Path, dest: Path, floor_text: str, ref: str) -> None:
    dest.mkdir(parents=True)
    _export_paths(repo_root, dest, ref, SUBSTRATE_EXPORTS)
    (dest / "CLAUDE.md").write_text(_fill(floor_text))
    (dest / "analytics" / "pyproject.toml").write_text(ARM_PYPROJECT)
    _write_settings(dest)


def build_manifest(arm: str, ref: str, dest: Path, layer_of: dict[str, str]) -> dict:
    files = {}
    skip = {".venv", "__pycache__"}
    for f in sorted(p for p in dest.rglob("*") if p.is_file()):
        if f.name == "manifest.json" or skip & set(f.relative_to(dest).parts):
            continue
        rel = str(f.relative_to(dest))
        files[rel] = {
            "sha256": hashlib.sha256(f.read_bytes()).hexdigest(),
            "layer": layer_of.get(rel, "substrate"),
        }
    return {"arm": arm, "ref": ref, "files": files}


def prep_arm(
    arm: str, repo_root: Path, arms_root: Path, floor_text: str, ref: str = "main", sync: bool = True
) -> Path:
    if arm not in ARM_LAYERS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {sorted(ARM_LAYERS)}")
    dest = arms_root / arm
    if dest.exists():
        shutil.rmtree(dest)
    build_substrate(repo_root, dest, floor_text, ref)
    layer_of: dict[str, str] = {}
    for layer in ARM_LAYERS[arm]:
        for extracted in _export_paths(repo_root, dest, ref, LAYERS[layer]):
            layer_of[extracted] = layer
    # Sync before the manifest so uv.lock (dependency resolution) is part of
    # run provenance: a re-prep that resolves different deps changes the
    # manifest, and run_matrix's arms_sha256 gate then refuses to mix it into
    # an existing batch. The .venv itself stays excluded (build artifacts).
    if sync:
        subprocess.run(["uv", "sync"], cwd=dest / "analytics", check=True)
    manifest = build_manifest(arm, ref, dest, layer_of)
    (dest / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return dest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--arms-root", type=Path, default=Path.home() / ".cache" / "gp_quality_bench" / "arms"
    )
    parser.add_argument("--ref", default="main")
    parser.add_argument(
        "--no-sync", action="store_true", help="skip `uv sync` in each arm (tests/CI; runs need synced arms)"
    )
    args = parser.parse_args()
    here = Path(__file__).parent
    repo_root = here.parents[2]
    floor_text = (here / "floor.md").read_text()
    canaries = integrity.load_canaries(here / "canaries.yaml")
    failures = integrity.check_canary_staleness(canaries, repo_root)
    failures += [f"floor: {f}" for f in integrity.check_text_leakage(floor_text, set(), canaries)]
    if failures:
        raise SystemExit("pre-prep integrity failed:\n" + "\n".join(failures))
    for arm in ARMS:
        path = prep_arm(arm, repo_root, args.arms_root, floor_text, ref=args.ref, sync=not args.no_sync)
        arm_failures = integrity.check_links(path)
        arm_failures += integrity.check_arm_leakage(path, set(ARM_LAYERS[arm]), canaries)
        if arm_failures:
            raise SystemExit(f"arm {arm!r} failed integrity:\n" + "\n".join(arm_failures))
        n = len(json.loads((path / "manifest.json").read_text())["files"])
        print(f"prepped {arm}: {path} ({n} files, integrity ok)")
