"""Question-bank and answer-key loading for the quality benchmark.

Keys are grader-side only: nothing in this module is ever copied into a run
worktree (prep_arms deletes the whole quality_bench dir from run arms).
Format documented in keys/KEY_SCHEMA.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

ARMS = ["full", "knowledge", "bare"]
PRODUCTS = {"win", "serve"}
TRAPS = {"none", "provenance", "overlap", "point_in_time", "denominator"}
SPLITS = {"calibration", "holdout"}


@dataclass(frozen=True)
class Question:
    id: str
    product: str
    trap: str
    split: str
    prompt_file: str
    key_file: str


@dataclass(frozen=True)
class NumberSpec:
    name: str
    value: float
    tolerance_pct: float


@dataclass(frozen=True)
class SourceCheck:
    id: str
    pattern: str
    description: str


@dataclass(frozen=True)
class Key:
    id: str
    as_of: str
    numbers: list[NumberSpec]
    required_resolutions: dict[str, str] = field(default_factory=dict)
    mandatory_sources: list[SourceCheck] = field(default_factory=list)
    severity1_patterns: list[str] = field(default_factory=list)
    required_assumptions: list[str] = field(default_factory=list)
    intent_card: str = ""


def load_manifest(path: Path) -> list[Question]:
    raw = yaml.safe_load(path.read_text())
    questions = []
    for row in raw["questions"]:
        q = Question(**row)
        if q.product not in PRODUCTS:
            raise ValueError(f"{q.id}: bad product {q.product!r}")
        if q.trap not in TRAPS:
            raise ValueError(f"{q.id}: bad trap {q.trap!r}")
        if q.split not in SPLITS:
            raise ValueError(f"{q.id}: bad split {q.split!r}")
        questions.append(q)
    return questions


def load_key(path: Path) -> Key:
    raw = yaml.safe_load(path.read_text())
    numbers = [NumberSpec(**n) for n in raw.get("numbers", [])]
    if not numbers:
        raise ValueError(f"{path}: key has no numbers")
    return Key(
        id=raw["id"],
        as_of=str(raw["as_of"]),
        numbers=numbers,
        required_resolutions=raw.get("required_resolutions", {}),
        mandatory_sources=[SourceCheck(**s) for s in raw.get("mandatory_sources", [])],
        severity1_patterns=raw.get("severity1_patterns", []),
        required_assumptions=raw.get("required_assumptions", []),
        intent_card=raw.get("intent_card", ""),
    )
