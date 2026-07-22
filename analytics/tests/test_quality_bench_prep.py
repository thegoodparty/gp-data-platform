import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from quality_bench import prep_arms

FLOOR = "# floor\nlib at {{LIB_PATH}} project {{UV_PROJECT}}\n"


@pytest.fixture
def fake_repo(tmp_path: Path) -> Path:
    """Mirror of the real layout: treatment skills/agents/libs, plus content
    that must NEVER reach an arm (quality_bench keys, data-matching skill,
    code-critic agent, dbt models)."""
    repo = tmp_path / "repo"
    files = {
        ".claude/skills/analytics-process/SKILL.md": "process prose [p](references/pipeline.md)",
        ".claude/skills/analytics-process/references/pipeline.md": "pipeline",
        ".claude/skills/win-analytics-knowledge/SKILL.md": "win prose [g](references/gotchas.md)",
        ".claude/skills/win-analytics-knowledge/references/gotchas.md": "win gotchas",
        ".claude/skills/serve-analytics-knowledge/SKILL.md": "serve prose",
        ".claude/skills/data-matching/SKILL.md": "matching prose",
        ".claude/agents/product-data-scientist.md": "pds prose",
        ".claude/agents/product-manager.md": "pm prose",
        ".claude/agents/code-critic.md": "critic prose",
        "analytics/lib/databricks_conn.py": "# conn",
        "analytics/lib/win_analysis.py": "# win helpers",
        "analytics/lib/serve_analysis.py": "# serve helpers",
        "analytics/diagnostics/quality_bench/keys/q01_key.yaml": "answer: 42",
        "dbt/project/models/marts/some_model.sql": "select 1",
        "CLAUDE.md": "repo claude md",
    }
    for rel, content in files.items():
        p = repo / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content)
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=repo, check=True)
    subprocess.run(["git", "add", "-A"], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init"], cwd=repo, check=True
    )
    return repo


def _prep(arm, fake_repo, tmp_path):
    return prep_arms.prep_arm(arm, fake_repo, tmp_path / "arms", FLOOR, sync=False)


def _manifest(arm_dir: Path) -> dict:
    return json.loads((arm_dir / "manifest.json").read_text())


def test_bare_is_substrate_only(fake_repo, tmp_path):
    arm = _prep("bare", fake_repo, tmp_path)
    assert (arm / "analytics" / "lib" / "databricks_conn.py").exists()
    assert (arm / "analytics" / "pyproject.toml").exists()
    assert not (arm / ".claude" / "skills").exists()
    assert not (arm / ".git").exists()
    claude_md = (arm / "CLAUDE.md").read_text()
    assert claude_md.startswith("# floor")
    assert "analytics/lib" in claude_md and "{{LIB_PATH}}" not in claude_md
    settings = json.loads((arm / ".claude" / "settings.local.json").read_text())
    assert settings["permissions"]["allow"]


def test_all_arms_share_identical_substrate(fake_repo, tmp_path):
    dirs = {a: _prep(a, fake_repo, tmp_path) for a in ("bare", "knowledge", "full")}
    texts = {a: (d / "CLAUDE.md").read_text() for a, d in dirs.items()}
    assert texts["bare"] == texts["knowledge"] == texts["full"]
    for d in dirs.values():
        assert (d / "analytics" / "lib" / "databricks_conn.py").exists()
        assert (d / "analytics" / "pyproject.toml").exists()


def test_knowledge_arm_adds_only_knowledge_layer(fake_repo, tmp_path):
    arm = _prep("knowledge", fake_repo, tmp_path)
    assert (arm / ".claude" / "skills" / "win-analytics-knowledge" / "SKILL.md").exists()
    assert (arm / ".claude" / "skills" / "serve-analytics-knowledge").exists()
    assert (arm / "analytics" / "lib" / "win_analysis.py").exists()
    assert (arm / "analytics" / "lib" / "serve_analysis.py").exists()
    assert not (arm / ".claude" / "skills" / "analytics-process").exists()
    assert not (arm / ".claude" / "skills" / "data-matching").exists()
    assert not (arm / ".claude" / "agents").exists()


def test_full_arm_adds_process_layer_and_claimed_reviewers_only(fake_repo, tmp_path):
    arm = _prep("full", fake_repo, tmp_path)
    assert (arm / ".claude" / "skills" / "analytics-process" / "SKILL.md").exists()
    assert (arm / ".claude" / "skills" / "win-analytics-knowledge").exists()
    assert (arm / ".claude" / "agents" / "product-data-scientist.md").exists()
    assert (arm / ".claude" / "agents" / "product-manager.md").exists()
    # Explicitly excluded treatments: not claimed by the pre-registered treatment.
    assert not (arm / ".claude" / "agents" / "code-critic.md").exists()
    assert not (arm / ".claude" / "skills" / "data-matching").exists()


def test_no_arm_contains_quality_bench_or_repo_extras(fake_repo, tmp_path):
    """Additive means absence by construction: keys, dbt models, and the repo
    CLAUDE.md content simply are not in any layer."""
    for arm_name in ("bare", "knowledge", "full"):
        arm = _prep(arm_name, fake_repo, tmp_path)
        assert not (arm / "analytics" / "diagnostics").exists()
        assert not (arm / "dbt").exists()
        assert not (arm / ".git").exists()
        assert "repo claude md" not in (arm / "CLAUDE.md").read_text()


def test_manifest_covers_every_file_with_layer_attribution(fake_repo, tmp_path):
    arm = _prep("full", fake_repo, tmp_path)
    manifest = _manifest(arm)
    assert manifest["arm"] == "full" and manifest["ref"] == "main"
    on_disk = {str(p.relative_to(arm)) for p in arm.rglob("*") if p.is_file() and p.name != "manifest.json"}
    assert set(manifest["files"]) == on_disk
    layers = {f["layer"] for f in manifest["files"].values()}
    assert layers == {"substrate", "knowledge", "process"}
    assert manifest["files"][".claude/skills/analytics-process/SKILL.md"]["layer"] == "process"
    assert manifest["files"]["analytics/lib/win_analysis.py"]["layer"] == "knowledge"
    assert manifest["files"]["analytics/lib/databricks_conn.py"]["layer"] == "substrate"
    assert all(len(f["sha256"]) == 64 for f in manifest["files"].values())


def test_manifest_is_deterministic(fake_repo, tmp_path):
    m1 = _manifest(_prep("knowledge", fake_repo, tmp_path))
    arm2 = prep_arms.prep_arm("knowledge", fake_repo, tmp_path / "arms2", FLOOR, sync=False)
    assert m1 == _manifest(arm2)


def test_unknown_arm_raises(fake_repo, tmp_path):
    with pytest.raises(ValueError):
        _prep("mystery", fake_repo, tmp_path)


def test_prep_survives_leftover_dir(fake_repo, tmp_path):
    arms_root = tmp_path / "arms"
    dest = arms_root / "full"
    dest.mkdir(parents=True)
    (dest / "leftover.txt").write_text("stale content from a crashed prior run")
    arm = prep_arms.prep_arm("full", fake_repo, arms_root, FLOOR, sync=False)
    assert arm == dest
    assert not (dest / "leftover.txt").exists()


def test_missing_layer_path_at_ref_raises(fake_repo, tmp_path):
    """A renamed skill must fail prep loudly, not silently build a thinner arm."""
    subprocess.run(
        ["git", "-C", str(fake_repo), "mv", ".claude/skills/analytics-process", ".claude/skills/renamed"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-c",
            "user.email=t@t",
            "-c",
            "user.name=t",
            "-C",
            str(fake_repo),
            "commit",
            "-qam",
            "rename",
        ],
        check=True,
    )
    with pytest.raises(RuntimeError):
        prep_arms.prep_arm("full", fake_repo, tmp_path / "arms", FLOOR, sync=False)


def test_one_factor_contrast_pinned_by_hashes(fake_repo, tmp_path):
    """The one-factor guarantee at the content level: substrate must be
    byte-identical (same hash) across all three arms, and every file the
    knowledge layer contributes to the knowledge arm must show up in the
    full arm with the same hash and layer label — full only ever adds on
    top of knowledge, never replaces it."""
    manifests = {a: _manifest(_prep(a, fake_repo, tmp_path)) for a in ("bare", "knowledge", "full")}

    substrate_by_arm = {
        arm: {rel: f["sha256"] for rel, f in m["files"].items() if f["layer"] == "substrate"}
        for arm, m in manifests.items()
    }
    rels = set(substrate_by_arm["bare"])
    assert rels and rels == set(substrate_by_arm["knowledge"]) == set(substrate_by_arm["full"])
    for rel in rels:
        hashes = {substrate_by_arm[a][rel] for a in ("bare", "knowledge", "full")}
        assert len(hashes) == 1, f"{rel} substrate hash diverges across arms: {hashes}"

    knowledge_files = manifests["knowledge"]["files"]
    full_files = manifests["full"]["files"]
    knowledge_layer_rels = [rel for rel, f in knowledge_files.items() if f["layer"] == "knowledge"]
    assert knowledge_layer_rels
    for rel in knowledge_layer_rels:
        assert rel in full_files
        assert full_files[rel]["sha256"] == knowledge_files[rel]["sha256"]
        assert full_files[rel]["layer"] == "knowledge"


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


def _run_prep_cli(
    fake_repo: Path, tmp_path: Path, canaries_yaml: str, arms_subdir: str = "arms"
) -> subprocess.CompletedProcess:
    """Run the prep CLI as a real subprocess against the fake repo, by copying
    the harness modules in. The CLI resolves repo_root from its own file
    location (`here.parents[2]`), so the harness must live at
    <repo>/analytics/diagnostics/quality_bench for that resolution to land on
    fake_repo instead of the real one."""
    dest = fake_repo / "analytics" / "diagnostics" / "quality_bench"
    dest.mkdir(parents=True, exist_ok=True)
    real_qb = Path(prep_arms.__file__).parent
    for name in ("prep_arms.py", "integrity.py", "bank.py", "__init__.py"):
        shutil.copy(real_qb / name, dest / name)
    (dest / "floor.md").write_text("# floor\nlib {{LIB_PATH}} proj {{UV_PROJECT}}\n")
    (dest / "canaries.yaml").write_text(canaries_yaml)
    return subprocess.run(
        [sys.executable, str(dest / "prep_arms.py"), "--arms-root", str(tmp_path / arms_subdir), "--no-sync"],
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_cli_passes_integrity_on_clean_repo(fake_repo, tmp_path):
    canaries_yaml = (
        'canaries:\n  - {layer: process, source: .claude/agents/product-manager.md, phrase: "pm prose"}\n'
    )
    proc = _run_prep_cli(fake_repo, tmp_path, canaries_yaml)
    assert proc.returncode == 0, proc.stderr
    assert "integrity ok" in proc.stdout


def test_cli_fails_when_treatment_canary_leaks_into_floor(fake_repo, tmp_path):
    """A clean run passes first; then floor.md is poisoned in place (the
    harness dir left behind by the clean run) and the CLI is rerun directly,
    to confirm the pre-prep floor-leakage gate fires on its own, not just as
    a side effect of a fresh copy."""
    canaries_yaml = (
        'canaries:\n  - {layer: process, source: .claude/agents/product-manager.md, phrase: "pm prose"}\n'
    )
    proc = _run_prep_cli(fake_repo, tmp_path, canaries_yaml, arms_subdir="arms")
    assert proc.returncode == 0, proc.stderr

    dest = fake_repo / "analytics" / "diagnostics" / "quality_bench"
    (dest / "floor.md").write_text("# floor with pm prose leaked in\n")
    proc = subprocess.run(
        [sys.executable, str(dest / "prep_arms.py"), "--arms-root", str(tmp_path / "arms2"), "--no-sync"],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode != 0
    assert "canary leaked" in proc.stderr


def test_cli_fails_on_stale_canary(fake_repo, tmp_path):
    canaries_yaml = (
        "canaries:\n  - {layer: process, source: .claude/agents/product-manager.md, "
        'phrase: "not actually in the file"}\n'
    )
    proc = _run_prep_cli(fake_repo, tmp_path, canaries_yaml)
    assert proc.returncode != 0
    assert "stale canary" in proc.stderr
