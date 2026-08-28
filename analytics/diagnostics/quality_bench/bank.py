"""Question-bank and answer-key loading for the quality benchmark.

Keys are grader-side only: nothing in this module is ever copied into a run
worktree (prep_arms deletes the whole quality_bench dir from run arms).
Format documented in keys/KEY_SCHEMA.md.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ARMS = ["full", "knowledge", "bare"]
PRODUCTS = {"win", "serve"}
TRAPS = {"none", "provenance", "overlap", "point_in_time", "denominator"}
SPLITS = {"calibration", "holdout"}

# Question ids become run ids and filesystem path components (runs/<run_id> is
# rmtree'd and recreated), so they must be safe as a single path segment.
QUESTION_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


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


def _check_bank_path(qid: str, label: str, value: str) -> None:
    """prompt_file/key_file are joined under questions/ and keys/; reject
    anything that could resolve outside those directories."""
    p = Path(value)
    if p.is_absolute() or ".." in p.parts:
        raise ValueError(f"{qid}: {label} must be a relative path inside the bank, got {value!r}")


def load_manifest(path: Path) -> list[Question]:
    raw = yaml.safe_load(path.read_text())
    questions = []
    seen: set[str] = set()
    for row in raw["questions"]:
        q = Question(**row)
        if not QUESTION_ID.fullmatch(q.id) or "__" in q.id:
            # run_ids are "<question_id>__<arm>__r<rep>"; grade.py recovers the
            # parts by splitting on "__", so a "__" in a question id is ambiguous,
            # and anything outside [a-z0-9_-] is unsafe as a path component.
            raise ValueError(f"{q.id!r}: question id must match [a-z0-9_-] with no '__'")
        if q.id in seen:
            # Duplicate ids collide on runs/<run_id> paths and state.json entries.
            raise ValueError(f"{q.id!r}: duplicate question id")
        seen.add(q.id)
        _check_bank_path(q.id, "prompt_file", q.prompt_file)
        _check_bank_path(q.id, "key_file", q.key_file)
        if q.product not in PRODUCTS:
            raise ValueError(f"{q.id}: bad product {q.product!r}")
        if q.trap not in TRAPS:
            raise ValueError(f"{q.id}: bad trap {q.trap!r}")
        if q.split not in SPLITS:
            raise ValueError(f"{q.id}: bad split {q.split!r}")
        questions.append(q)
    return questions


def load_key(path: Path, expected_id: str | None = None) -> Key:
    raw = yaml.safe_load(path.read_text())
    numbers = [NumberSpec(**n) for n in raw.get("numbers", [])]
    if not numbers:
        raise ValueError(f"{path}: key has no numbers")
    if expected_id is not None and raw["id"] != expected_id:
        # A key silently grading the wrong question is worse than a crash.
        raise ValueError(f"{path}: key id {raw['id']!r} != question id {expected_id!r}")
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
