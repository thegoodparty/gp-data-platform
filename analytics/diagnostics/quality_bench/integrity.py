# analytics/diagnostics/quality_bench/integrity.py
"""Post-prep integrity checks (DATA-2164): dead-link scan, canary staleness
and leakage. Grader-side only — quality_bench/ is not in any arm layer, so
none of this (including canaries.yaml) ever ships into an arm.

Canary model: canaries.yaml lists distinctive verbatim phrases from treatment
content, tagged with the layer that owns them (knowledge | process). A phrase
appearing in the floor or in an arm that does not include its layer means
treatment content leaked through the substrate or the floor generator.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import yaml

LINK_RE = re.compile(r"\]\(([^)]+)\)")


@dataclass(frozen=True)
class Canary:
    layer: str
    source: str  # repo-relative path the phrase lives in
    phrase: str  # verbatim substring


def load_canaries(path: Path) -> list[Canary]:
    data = yaml.safe_load(path.read_text())
    return [Canary(**c) for c in data["canaries"]]


def check_canary_staleness(canaries: list[Canary], repo_root: Path) -> list[str]:
    """A canary whose phrase no longer exists in its source is dead weight:
    the leakage scan would be grepping for nothing. Fails loudly instead."""
    failures = []
    for c in canaries:
        src = repo_root / c.source
        if not src.is_file() or c.phrase not in src.read_text(errors="ignore"):
            failures.append(f"stale canary: {c.source}: {c.phrase!r}")
    return failures


def check_text_leakage(text: str, allowed_layers: set[str], canaries: list[Canary]) -> list[str]:
    return [
        f"canary leaked ({c.layer}): {c.phrase!r}"
        for c in canaries
        if c.layer not in allowed_layers and c.phrase in text
    ]
