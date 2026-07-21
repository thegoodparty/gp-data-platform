"""Deterministic grading layers: results-block parsing, number/source/severity
checks, and cross-rep consistency. Pure functions; no subprocesses (judge.py
owns the judged layer)."""

from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass

import yaml

from quality_bench.bank import Key

YAML_FENCE = re.compile(r"```ya?ml\n(.*?)```", re.DOTALL)


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    kind: str  # results_block | number | source | severity1
    passed: bool
    detail: str


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
        if spec.value:
            diff_pct = abs(value - spec.value) / abs(spec.value) * 100
        else:
            # Zero key: exact zero passes; anything else has no meaningful pct diff.
            diff_pct = 0.0 if value == 0.0 else float("inf")
        passed = diff_pct <= spec.tolerance_pct
        out.append(
            CheckResult(
                spec.name, "number", passed, f"got {value:g}, key {spec.value:g}, diff {diff_pct:.2f}%"
            )
        )
    return out


def check_sources(transcript_text: str, key: Key) -> list[CheckResult]:
    out = []
    for src in key.mandatory_sources:
        found = re.search(src.pattern, transcript_text, re.IGNORECASE) is not None
        out.append(CheckResult(src.id, "source", found, src.description))
    return out


def check_severity1(answer_text: str, key: Key) -> list[CheckResult]:
    out = []
    for i, pattern in enumerate(key.severity1_patterns):
        matched = re.search(pattern, answer_text, re.IGNORECASE) is not None
        out.append(
            CheckResult(f"severity1_{i}", "severity1", not matched, f"tripwire {pattern!r} matched={matched}")
        )
    return out


def check_assumptions(block: dict | None, key: Key) -> list[CheckResult]:
    """Each required_assumptions fork must be surfaced in the answer's assumptions
    ledger with a non-empty resolution: a bare fork entry says nothing about how
    the fork was actually resolved."""
    ledger = _resolutions(block) if block else {}
    out = []
    for fork in key.required_assumptions:
        resolution = ledger.get(fork, "").strip()
        detail = f"resolved as {resolution!r}" if resolution else "fork missing or resolution empty"
        out.append(CheckResult(fork, "assumption", bool(resolution), detail))
    return out


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


def check_resolutions(block: dict | None, key: Key) -> list[CheckResult]:
    """Compare each resolved fork against the key's expected value (normalized
    containment either way — models phrase resolutions freely). Reported as the
    resolutions_match column, NOT gated into the verdict rules: cross-rep
    agreement (cell_consistency) is deliberately correctness-blind, and the
    deterministic instrument for a wrong resolution is the numbers themselves —
    key tolerances must be tight enough that the wrong fork's number fails."""
    ledger = _resolutions(block) if block else {}
    out = []
    for fork, expected in key.required_resolutions.items():
        got = _norm(ledger.get(fork, ""))
        want = _norm(expected)
        matched = bool(got) and (want in got or got in want)
        out.append(
            CheckResult(fork, "resolution", matched, f"resolved {ledger.get(fork, '')!r}, key {expected!r}")
        )
    return out


def _resolutions(block: dict) -> dict[str, str]:
    out = {}
    for a in block.get("results", {}).get("assumptions", []) or []:
        if isinstance(a, dict) and "fork" in a:
            # `or ""` maps a YAML-null resolution to empty, not the string "None".
            out[str(a["fork"])] = str(a.get("resolution") or "")
    return out


def cell_consistency(blocks: list[dict | None], key: Key) -> dict:
    parsed = [b for b in blocks if b]
    spreads: dict[str, float] = {}
    tol = {n.name: n.tolerance_pct for n in key.numbers}
    for name in tol:
        vals = []
        for b in parsed:
            v = b.get("results", {}).get("numbers", {}).get(name)
            with contextlib.suppress(TypeError, ValueError):
                vals.append(float(v))
        if len(vals) >= 2:
            mean = sum(vals) / len(vals)
            if mean:
                spreads[name] = (max(vals) - min(vals)) / abs(mean) * 100
            else:
                # Zero mean: identical reps (all zero) are perfectly consistent.
                spreads[name] = 0.0 if max(vals) == min(vals) else float("inf")
    forks = set(key.required_resolutions)
    agreement = {}
    for fork in forks:
        seen = {_resolutions(b).get(fork) for b in parsed}
        agreement[fork] = len(seen) == 1 and None not in seen
    numbers_ok = all(spreads.get(n, float("inf")) <= t for n, t in tol.items()) if parsed else False
    max_spread = max(spreads.get(n.name, float("inf")) for n in key.numbers) if parsed else float("inf")
    return {
        "n_reps": len(blocks),
        "n_parsed": len(parsed),
        "number_spread_pct": spreads,
        "max_spread_pct": max_spread,
        "resolution_agreement": agreement,
        "consistent": bool(parsed) and numbers_ok and all(agreement.values()),
    }
