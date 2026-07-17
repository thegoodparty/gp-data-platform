import json
import subprocess
import sys
from pathlib import Path

import pytest
from quality_bench import prep_arms

FLOOR = "# floor\nlib at {{LIB_PATH}} project {{UV_PROJECT}}\n"


@pytest.fixture
def fake_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / ".claude" / "skills" / "analytics-process").mkdir(parents=True)
    (repo / ".claude" / "skills" / "win-analytics-knowledge").mkdir(parents=True)
    (repo / ".claude" / "skills" / "serve-analytics-knowledge").mkdir(parents=True)
    (repo / ".claude" / "agents").mkdir(parents=True)
    (repo / "analytics" / "diagnostics" / "quality_bench" / "keys").mkdir(parents=True)
    (repo / "analytics" / "lib").mkdir(parents=True)
    for f in [
        ".claude/skills/analytics-process/SKILL.md",
        ".claude/skills/win-analytics-knowledge/SKILL.md",
        ".claude/skills/serve-analytics-knowledge/SKILL.md",
        ".claude/agents/reviewer.md",
        "analytics/diagnostics/quality_bench/keys/q01_key.yaml",
        "analytics/lib/databricks_conn.py",
        "CLAUDE.md",
    ]:
        (repo / f).write_text("x")
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"], cwd=repo, check=True
    )
    return repo


def test_full_arm_excludes_quality_bench_keeps_skills(fake_repo, tmp_path):
    arm = prep_arms.prep_arm("full", fake_repo, tmp_path / "arms", FLOOR, sync=False)
    assert not (arm / "analytics" / "diagnostics" / "quality_bench").exists()
    # git-archive export, not a worktree: no .git, so the deleted keys are not
    # recoverable from history.
    assert not (arm / ".git").exists()
    assert (arm / ".claude" / "skills" / "analytics-process").exists()
    assert "{{LIB_PATH}}" not in (arm / "CLAUDE.md").read_text()
    assert "analytics/lib" in (arm / "CLAUDE.md").read_text()
    settings = json.loads((arm / ".claude" / "settings.local.json").read_text())
    assert settings["permissions"]["allow"]


def test_knowledge_arm_prunes_process_skill_and_agents(fake_repo, tmp_path):
    arm = prep_arms.prep_arm("knowledge", fake_repo, tmp_path / "arms", FLOOR, sync=False)
    assert not (arm / ".claude" / "skills" / "analytics-process").exists()
    assert not (arm / ".claude" / "agents").exists()
    assert (arm / ".claude" / "skills" / "win-analytics-knowledge").exists()
    assert (arm / ".claude" / "skills" / "serve-analytics-knowledge").exists()
    settings = json.loads((arm / ".claude" / "settings.local.json").read_text())
    assert settings["permissions"]["allow"]


def test_bare_arm_is_scratch_with_floor_and_env(fake_repo, tmp_path):
    arm = prep_arms.prep_arm("bare", fake_repo, tmp_path / "arms", FLOOR, sync=False)
    assert not (arm / ".git").exists()
    assert not (arm / ".claude" / "skills").exists()
    claude_md = (arm / "CLAUDE.md").read_text()
    assert claude_md.startswith("# floor")
    assert "env" in claude_md
    assert (arm / "env" / "databricks_conn.py").exists()
    assert (arm / "env" / "pyproject.toml").exists()
    settings = json.loads((arm / ".claude" / "settings.local.json").read_text())
    assert settings["permissions"]["allow"]


def test_unknown_arm_raises(fake_repo, tmp_path):
    with pytest.raises(ValueError):
        prep_arms.prep_arm("mystery", fake_repo, tmp_path / "arms", FLOOR, sync=False)


def test_full_arm_survives_leftover_non_worktree_dir(fake_repo, tmp_path):
    arms_root = tmp_path / "arms"
    dest = arms_root / "full"
    dest.mkdir(parents=True)
    (dest / "leftover.txt").write_text("stale content from a crashed prior run")

    arm = prep_arms.prep_arm("full", fake_repo, arms_root, FLOOR, sync=False)

    assert arm == dest
    assert not (dest / "leftover.txt").exists()
    assert not (arm / "analytics" / "diagnostics" / "quality_bench").exists()
    assert (arm / ".claude" / "skills" / "analytics-process").exists()


def test_cli_help_works_under_bare_python():
    analytics_root = Path(__file__).parent.parent
    proc = subprocess.run(
        [sys.executable, "diagnostics/quality_bench/prep_arms.py", "--help"],
        cwd=analytics_root,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert "--arms-root" in proc.stdout
