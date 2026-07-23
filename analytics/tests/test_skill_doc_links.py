"""Link checker for the analytics skill doc graph (DATA-2113).

The process/knowledge skills follow a one-fact-one-home rule, so the docs lean on
relative markdown links (including fragile cross-skill paths) instead of restating
facts. A renamed or moved reference doc breaks those links silently. This test walks
every markdown file in the analytics process/knowledge skills plus the reviewer
agents and asserts each relative markdown link resolves to an existing file.
"""

from pathlib import Path

from quality_bench.integrity import relative_md_targets as _relative_md_targets

REPO_ROOT = Path(__file__).resolve().parents[2]

DOC_GLOBS = [
    ".claude/skills/analytics-process/**/*.md",
    ".claude/skills/*-analytics-knowledge/**/*.md",
    ".claude/agents/product-*.md",
]


def _doc_files() -> list[Path]:
    files: list[Path] = []
    for pattern in DOC_GLOBS:
        files.extend(REPO_ROOT.glob(pattern))
    return sorted(files)


def test_doc_files_found():
    files = _doc_files()
    assert files, "no skill docs matched - the glob or repo layout changed"
    assert any("analytics-process" in str(f) for f in files)
    assert any("win-analytics-knowledge" in str(f) for f in files)
    assert any("serve-analytics-knowledge" in str(f) for f in files)


def test_relative_markdown_links_resolve():
    broken = []
    for doc in _doc_files():
        for target in _relative_md_targets(doc.read_text()):
            resolved = (doc.parent / target).resolve()
            if not resolved.is_file():
                broken.append(f"{doc.relative_to(REPO_ROOT)} -> {target}")
    assert not broken, "broken relative links:\n" + "\n".join(broken)


KNOWLEDGE_SKILL_DIRS = [
    REPO_ROOT / ".claude" / "skills" / "win-analytics-knowledge",
    REPO_ROOT / ".claude" / "skills" / "serve-analytics-knowledge",
]


def test_knowledge_skills_are_self_contained():
    """DATA-2164: the knowledge skills ship into bench arms without the
    process or data-matching skills present, so their relative links must not
    escape their own skill directory. Cross-skill pointers are plain text."""
    escapes = []
    for skill_dir in KNOWLEDGE_SKILL_DIRS:
        for md in sorted(skill_dir.rglob("*.md")):
            for target in _relative_md_targets(md.read_text()):
                resolved = (md.parent / target).resolve()
                if not resolved.is_relative_to(skill_dir.resolve()):
                    escapes.append(f"{md.relative_to(REPO_ROOT)} -> {target}")
    assert not escapes, "cross-skill links break arm self-containment:\n" + "\n".join(escapes)
