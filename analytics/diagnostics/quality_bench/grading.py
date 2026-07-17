"""Deterministic grading layers: results-block parsing, number/source/severity
checks, and cross-rep consistency. Pure functions; no subprocesses (judge.py
owns the judged layer)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path

import yaml
from quality_bench.bank import Key

YAML_FENCE = re.compile(r"```ya?ml\n(.*?)```", re.DOTALL)


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    kind: str  # results_block | number | source | severity1
    passed: bool
    detail: str


def final_answer_text(transcript_path: Path) -> str:
    """Last assistant text message in a Claude Code JSONL transcript."""
    last = ""
    for line in transcript_path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if row.get("type") != "assistant":
            continue
        for part in row.get("message", {}).get("content", []):
            if part.get("type") == "text" and part.get("text"):
                last = part["text"]
    return last


def parse_results_block(answer_text: str) -> dict | None:
    """Last ```yaml fence whose top level contains `results:`."""
    for match in reversed(YAML_FENCE.findall(answer_text)):
        try:
            parsed = yaml.safe_load(match)
        except yaml.YAMLError:
            continue
        if isinstance(parsed, dict) and isinstance(parsed.get("results"), dict):
            return parsed
    return None


def check_numbers(block: dict | None, key: Key) -> list[CheckResult]:
    got = (block or {}).get("results", {}).get("numbers", {}) or {}
    out = []
    for spec in key.numbers:
        if spec.name not in got:
            out.append(CheckResult(spec.name, "number", False, "missing from results block"))
            continue
        try:
            value = float(got[spec.name])
        except (TypeError, ValueError):
            out.append(CheckResult(spec.name, "number", False, f"non-numeric: {got[spec.name]!r}"))
            continue
        diff_pct = abs(value - spec.value) / abs(spec.value) * 100 if spec.value else float("inf")
        passed = diff_pct <= spec.tolerance_pct
        out.append(
            CheckResult(
                spec.name, "number", passed, f"got {value:g}, key {spec.value:g}, diff {diff_pct:.2f}%"
            )
        )
    return out
