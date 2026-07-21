# analytics/diagnostics/quality_bench/prep_arms.py
"""Build the three arm environments (design §6).

Contamination rules enforced here:
- quality_bench/ (keys, questions, harness) is deleted from every run arm.
- full/knowledge arms are `git archive` exports, not worktrees: the arm is a
  plain directory with no .git and no history, so the answer keys are not
  recoverable from git even though quality_bench was deleted from the checkout.
- Arms live at fresh paths, so project-keyed auto-memory does not attach.
- bare is a plain scratch dir: floor only, no repo content, no skills.
"""

from __future__ import annotations

import argparse
import io
import json
import shutil
import subprocess
import tarfile
from pathlib import Path

try:
    from quality_bench.bank import ARMS
except ImportError:  # bare `python prep_arms.py`: diagnostics/ isn't on sys.path yet
    import sys

    sys.path.insert(0, str(Path(__file__).parents[1]))
    from quality_bench.bank import ARMS

KNOWLEDGE_SKILLS = ["win-analytics-knowledge", "serve-analytics-knowledge"]

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

BARE_PYPROJECT = """\
[project]
name = "bench-env"
version = "0.1.0"
requires-python = ">=3.14"
dependencies = ["pandas>=2.3.1", "databricks-sql-connector>=3", "databricks-sdk>=0.20"]

[tool.uv]
package = false
"""


def _fill(floor_text: str, lib_path: str, uv_project: str) -> str:
    return floor_text.replace("{{LIB_PATH}}", lib_path).replace("{{UV_PROJECT}}", uv_project)


def _write_settings(arm_dir: Path) -> None:
    d = arm_dir / ".claude"
    d.mkdir(parents=True, exist_ok=True)
    (d / "settings.local.json").write_text(json.dumps(SETTINGS_JSON, indent=2))


def _export(repo_root: Path, dest: Path, ref: str) -> None:
    """Export the repo at `ref` into `dest` as a plain directory (no .git).

    `git archive` streams a tar of the tracked tree at `ref`; extracting it gives
    the arm the repo content without any git history, so the deleted answer keys
    stay unrecoverable. Any leftover dir (e.g. a crashed prior run) is cleared
    first via rmtree.
    """
    if dest.exists():
        shutil.rmtree(dest)
    proc = subprocess.run(
        ["git", "archive", ref],
        cwd=repo_root,
        check=False,
        capture_output=True,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"git archive failed for {ref} in {repo_root}: {stderr}")
    dest.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(proc.stdout)) as tar:
        tar.extractall(dest, filter="data")


def prep_arm(
    arm: str, repo_root: Path, arms_root: Path, floor_text: str, ref: str = "main", sync: bool = True
) -> Path:
    if arm not in ARMS:
        raise ValueError(f"unknown arm {arm!r}; expected one of {ARMS}")
    dest = arms_root / arm

    if arm == "bare":
        if dest.exists():
            shutil.rmtree(dest)
        env = dest / "env"
        env.mkdir(parents=True)
        (dest / "CLAUDE.md").write_text(_fill(floor_text, "env", "env"))
        shutil.copy(repo_root / "analytics" / "lib" / "databricks_conn.py", env / "databricks_conn.py")
        (env / "pyproject.toml").write_text(BARE_PYPROJECT)
        _write_settings(dest)
        if sync:
            subprocess.run(["uv", "sync"], cwd=env, check=True)
        return dest

    _export(repo_root, dest, ref)
    shutil.rmtree(dest / "analytics" / "diagnostics" / "quality_bench", ignore_errors=True)
    claude_md = dest / "CLAUDE.md"
    existing = claude_md.read_text() if claude_md.exists() else ""
    claude_md.write_text(existing + "\n\n" + _fill(floor_text, "analytics/lib", "analytics"))

    if arm == "knowledge":
        skills = dest / ".claude" / "skills"
        for child in skills.iterdir() if skills.exists() else []:
            if child.name not in KNOWLEDGE_SKILLS:
                shutil.rmtree(child)
        shutil.rmtree(dest / ".claude" / "agents", ignore_errors=True)

    _write_settings(dest)
    if sync:
        subprocess.run(["uv", "sync"], cwd=dest / "analytics", check=True)
    return dest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--arms-root", type=Path, default=Path.home() / ".cache" / "gp_quality_bench" / "arms"
    )
    parser.add_argument("--ref", default="main")
    args = parser.parse_args()
    here = Path(__file__).parent
    repo_root = here.parents[2]
    floor_text = (here / "floor.md").read_text()
    for arm in ARMS:
        path = prep_arm(arm, repo_root, args.arms_root, floor_text, ref=args.ref)
        print(f"prepped {arm}: {path}")
