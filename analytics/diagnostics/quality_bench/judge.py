"""Blind judged layer (design §7). The judge sees the answer, the key, and a
rubric — never the arm, run id, or transcript. Judge scores can never override
deterministic failures (enforced in grade.py, which combines layers)."""

from __future__ import annotations

import json
import subprocess
import tempfile

import yaml

from quality_bench.bank import Key
from quality_bench.grading import YAML_FENCE

RUBRIC_DIMENSIONS = ["scoping_correctness", "confident_wrongness", "caveat_quality", "assumptions_surfaced"]

PROMPT_TEMPLATE = """You are grading an analytics answer against a fixed answer key. You do not
know what produced the answer; grade only what is written.

## Answer key
Correct numbers (tolerance in %):
{numbers}
Required scoping resolutions:
{resolutions}
Known confidently-wrong claims to look for (semantic match, not just literal):
{severity1}

## Rubric — score each 0-2
- scoping_correctness: resolutions used match the key (2 = all, 1 = partial, 0 = wrong)
- confident_wrongness: 2 = no wrong claim stated as fact; 0 = a known-wrong or
  demonstrably false claim asserted confidently; 1 = wrong but hedged
- caveat_quality: are limitations/uncertainty honestly stated where warranted
- assumptions_surfaced: does the answer expose its scoping decisions clearly

## Answer to grade
{answer}

## Output
End with exactly one fenced yaml block:
```yaml
judge:
  scores:
    scoping_correctness: <0-2>
    confident_wrongness: <0-2>
    caveat_quality: <0-2>
    assumptions_surfaced: <0-2>
  severity1_found: <true|false>
  rationale: <one line>
```
"""


def build_judge_prompt(key: Key, answer_text: str, run_context: str = "") -> str:
    # run_context is accepted and deliberately IGNORED: the judge is blind.
    del run_context
    numbers = "\n".join(f"- {n.name}: {n.value:g} (±{n.tolerance_pct}%)" for n in key.numbers)
    resolutions = "\n".join(f"- {k}: {v}" for k, v in key.required_resolutions.items()) or "- (none)"
    severity1 = "\n".join(f"- {p}" for p in key.severity1_patterns) or "- (none known)"
    return PROMPT_TEMPLATE.format(
        numbers=numbers, resolutions=resolutions, severity1=severity1, answer=answer_text
    )


def parse_judge_output(text: str) -> dict | None:
    """Parse the judge's fenced YAML output. Extracts the last `judge:` block; echoed fences could shadow the real verdict."""
    for match in reversed(YAML_FENCE.findall(text)):
        try:
            parsed = yaml.safe_load(match)
        except yaml.YAMLError:
            continue
        if not (isinstance(parsed, dict) and "judge" in parsed and isinstance(parsed.get("judge"), dict)):
            continue
        j = parsed["judge"]
        scores = j.get("scores", {})
        if all(dim in scores for dim in RUBRIC_DIMENSIONS) and "severity1_found" in j:
            return j
    return None


def judge_answer(
    key: Key, answer_text: str, model: str, timeout_s: int = 600, runner=subprocess.run
) -> dict | None:
    prompt = build_judge_prompt(key, answer_text)
    with tempfile.TemporaryDirectory() as scratch:
        proc = runner(
            ["claude", "-p", prompt, "--output-format", "json", "--model", model, "--tools", ""],
            cwd=scratch,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    if proc.returncode != 0:
        return None
    try:
        result = json.loads(proc.stdout).get("result", "")
    except json.JSONDecodeError:
        return None
    return parse_judge_output(result)
