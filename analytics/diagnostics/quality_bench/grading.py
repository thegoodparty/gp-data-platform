"""Deterministic grading layers: results-block parsing, number/source/severity
checks, and cross-rep consistency. Pure functions; no subprocesses (judge.py
owns the judged layer)."""

from __future__ import annotations

import contextlib
import itertools
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


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()


# Tokens too generic to indicate two fork names/resolutions are about the same
# thing (every ledger entry has a "definition" or touches "users").
_GENERIC_TOKENS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "by",
    "de",
    "definition",
    "for",
    "handling",
    "in",
    "is",
    "no",
    "not",
    "of",
    "on",
    "or",
    "per",
    "the",
    "to",
    "via",
    "with",
}


def _tokens(s: str) -> set[str]:
    return {t for t in _norm(s).split() if t not in _GENERIC_TOKENS}


def _tokens_match(a: str, b: str) -> bool:
    # Same word, or same 4-char prefix (registered/registration, created/creation,
    # user/users) — cheap inflection tolerance without a stemmer.
    return a == b or (len(a) >= 4 and len(b) >= 4 and a[:4] == b[:4])


def _token_overlap(a: set[str], b: set[str]) -> int:
    return sum(1 for x in a if any(_tokens_match(x, y) for y in b))


def _find_fork(key_fork: str, ledger: dict[str, str]) -> str | None:
    """Best ledger fork name for a key fork. Answers invent their own fork slugs
    (the floor deliberately does not leak the key's names), so exact lookup is
    the fast path and token overlap the real one: `account_created` should find
    `account_creation_timestamp`, not miss."""
    if key_fork in ledger:
        return key_fork
    want = _tokens(key_fork)
    best, best_score = None, 0
    for name in ledger:
        score = _token_overlap(want, _tokens(name))
        if score > best_score:
            best, best_score = name, score
    return best


def check_assumptions(block: dict | None, key: Key) -> list[CheckResult]:
    """Each required_assumptions fork must be surfaced in the answer's assumptions
    ledger with a non-empty resolution: a bare fork entry says nothing about how
    the fork was actually resolved. Fork names are matched by token overlap, not
    exact string (see _find_fork)."""
    ledger = _resolutions(block) if block else {}
    out = []
    for fork in key.required_assumptions:
        name = _find_fork(fork, ledger)
        resolution = ledger.get(name, "").strip() if name else ""
        detail = f"as {name!r}: {resolution!r}" if resolution else "fork missing or resolution empty"
        out.append(CheckResult(fork, "assumption", bool(resolution), detail))
    return out


def _content_match(got: str, want: str) -> bool:
    g, w = _norm(got), _norm(want)
    if bool(g) and (w in g or g in w):
        return True
    # Free-phrased resolutions rarely containment-match a key slug; two
    # non-generic token hits is the lenient reported-only fallback.
    return _token_overlap(_tokens(got), _tokens(want)) >= 2


def check_resolutions(block: dict | None, key: Key) -> list[CheckResult]:
    """Compare each resolved fork against the key's expected value (normalized
    containment or token overlap — models phrase resolutions freely). Reported as
    the resolutions_match column, NOT gated into the verdict rules: cross-rep
    agreement (cell_consistency) is deliberately correctness-blind, and the
    deterministic instrument for a wrong resolution is the numbers themselves —
    key tolerances must be tight enough that the wrong fork's number fails."""
    ledger = _resolutions(block) if block else {}
    out = []
    for fork, expected in key.required_resolutions.items():
        name = _find_fork(fork, ledger)
        got = ledger.get(name, "") if name else ""
        # No name match anywhere: fall back to scanning every entry's content,
        # so a fork filed under an unrecognizable slug still gets credit.
        matched = (
            _content_match(got, expected)
            if got
            else any(_content_match(r, expected) for r in ledger.values())
        )
        out.append(CheckResult(fork, "resolution", matched, f"resolved {got!r}, key {expected!r}"))
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
    # Resolution agreement is key-blind AND reported-only: reps invent their own
    # fork names and phrase resolutions freely, so the comparable set is fork
    # names two or more reps both surfaced, compared fuzzily. It never gates
    # `consistent`: key tolerances are set so a divergent fork choice moves the
    # number, making numeric spread the deterministic instrument (same argument
    # as check_resolutions).
    ledgers = [_resolutions(b) for b in parsed]
    shared = {f for i, led in enumerate(ledgers) for f in led if any(f in o for o in ledgers[i + 1 :])}
    agreement = {}
    for fork in shared:
        seen = [led[fork] for led in ledgers if fork in led]
        agreement[fork] = all(_content_match(a, b) for a, b in itertools.pairwise(seen))
    numbers_ok = all(spreads.get(n, float("inf")) <= t for n, t in tol.items()) if parsed else False
    max_spread = max(spreads.get(n.name, float("inf")) for n in key.numbers) if parsed else float("inf")
    return {
        "n_reps": len(blocks),
        "n_parsed": len(parsed),
        "number_spread_pct": spreads,
        "max_spread_pct": max_spread,
        "resolution_agreement": agreement,
        "consistent": bool(parsed) and numbers_ok,
    }
