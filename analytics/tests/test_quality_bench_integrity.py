from pathlib import Path

from quality_bench import integrity

QB_DIR = Path(__file__).parent.parent / "diagnostics" / "quality_bench"
REPO_ROOT = Path(__file__).resolve().parents[2]

CANARIES_YAML = """\
canaries:
  - layer: knowledge
    source: skills/win/SKILL.md
    phrase: "the moon is made of governed cheese"
  - layer: process
    source: skills/process/SKILL.md
    phrase: "adversarial reviewer cadence"
"""


def _write_canaries(tmp_path: Path) -> Path:
    p = tmp_path / "canaries.yaml"
    p.write_text(CANARIES_YAML)
    return p


def test_load_canaries(tmp_path):
    canaries = integrity.load_canaries(_write_canaries(tmp_path))
    assert len(canaries) == 2
    assert canaries[0].layer == "knowledge"
    assert canaries[0].phrase == "the moon is made of governed cheese"


def test_staleness_flags_missing_phrase_and_missing_file(tmp_path):
    canaries = integrity.load_canaries(_write_canaries(tmp_path))
    repo = tmp_path / "repo"
    (repo / "skills" / "win").mkdir(parents=True)
    (repo / "skills" / "win" / "SKILL.md").write_text("the moon is made of governed cheese, per DATA-1")
    # process source file absent entirely
    failures = integrity.check_canary_staleness(canaries, repo)
    assert len(failures) == 1
    assert "skills/process/SKILL.md" in failures[0]


def test_staleness_passes_when_all_present(tmp_path):
    canaries = integrity.load_canaries(_write_canaries(tmp_path))
    repo = tmp_path / "repo"
    (repo / "skills" / "win").mkdir(parents=True)
    (repo / "skills" / "process").mkdir(parents=True)
    (repo / "skills" / "win" / "SKILL.md").write_text("x the moon is made of governed cheese y")
    (repo / "skills" / "process" / "SKILL.md").write_text("follow the adversarial reviewer cadence")
    assert integrity.check_canary_staleness(canaries, repo) == []


def test_text_leakage_respects_allowed_layers(tmp_path):
    canaries = integrity.load_canaries(_write_canaries(tmp_path))
    text = "inventory says the moon is made of governed cheese"
    assert integrity.check_text_leakage(text, set(), canaries)  # bare/floor: leak
    assert integrity.check_text_leakage(text, {"knowledge"}, canaries) == []  # knowledge arm: allowed


def test_real_canaries_are_fresh():
    """Every canary in the committed canaries.yaml exists verbatim in its
    source file, so the leakage scan is actually scanning for live treatment
    content. A reworded skill must break this test, forcing a canary refresh."""
    canaries = integrity.load_canaries(QB_DIR / "canaries.yaml")
    assert len(canaries) >= 8
    layers = {c.layer for c in canaries}
    assert layers == {"knowledge", "process"}
    assert integrity.check_canary_staleness(canaries, REPO_ROOT) == []


def test_check_links_flags_dead_relative_md_link(tmp_path):
    arm = tmp_path / "arm"
    (arm / "docs").mkdir(parents=True)
    (arm / "docs" / "a.md").write_text("see [b](b.md) and [gone](../missing/c.md)")
    (arm / "docs" / "b.md").write_text("fine")
    failures = integrity.check_links(arm)
    assert len(failures) == 1
    assert "missing/c.md" in failures[0]


def test_check_links_ignores_external_and_fragment_links(tmp_path):
    arm = tmp_path / "arm"
    arm.mkdir()
    (arm / "a.md").write_text("[x](https://example.com/p.md) [y](#anchor) [z](mailto:a@b.md)")
    assert integrity.check_links(arm) == []


def test_check_links_flags_link_escaping_arm_to_sibling(tmp_path):
    arm = tmp_path / "arm"
    arm.mkdir()
    (tmp_path / "outside.md").write_text("sibling arm content")
    (arm / "escape.md").write_text("see [out](../outside.md)")
    failures = integrity.check_links(arm)
    assert len(failures) == 1
    assert "outside.md" in failures[0]


def test_check_arm_leakage_scans_all_files(tmp_path):
    canaries = integrity.load_canaries(_write_canaries(tmp_path))
    arm = tmp_path / "arm"
    (arm / "analytics" / "lib").mkdir(parents=True)
    (arm / "analytics" / "lib" / "notes.py").write_text("# the moon is made of governed cheese")
    hits = integrity.check_arm_leakage(arm, set(), canaries)
    assert len(hits) == 1 and "notes.py" in hits[0]
    assert integrity.check_arm_leakage(arm, {"knowledge"}, canaries) == []
