"""Link checker for the analytics skill doc graph (DATA-2113).

The process/knowledge skills follow a one-fact-one-home rule, so the docs lean on
relative markdown links (including fragile cross-skill paths) instead of restating
facts. A renamed or moved reference doc breaks those links silently. This test walks
every markdown file in the analytics process/knowledge skills plus the reviewer
agents and asserts each relative markdown link resolves to an existing file.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

DOC_GLOBS = [
    ".claude/skills/analytics-process/**/*.md",
    ".claude/skills/*-analytics-knowledge/**/*.md",
    ".claude/agents/product-*.md",
]

LINK_RE = re.compile(r"\]\(([^)]+)\)")


def _doc_files() -> list[Path]:
    files: list[Path] = []
    for pattern in DOC_GLOBS:
        files.extend(REPO_ROOT.glob(pattern))
    return sorted(files)


def _relative_md_targets(text: str):
    """Yield relative .md link targets, skipping external URLs and pure fragments."""
    for match in LINK_RE.finditer(text):
        target = match.group(1).split("#", 1)[0].strip()
        if not target or target.startswith(("http://", "https://", "mailto:", "/")):
            continue
        if target.endswith(".md"):
            yield target


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
