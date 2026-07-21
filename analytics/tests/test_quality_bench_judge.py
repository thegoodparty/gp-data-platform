import json
import subprocess

from quality_bench import judge
from quality_bench.bank import Key, NumberSpec

KEY = Key(
    id="q01",
    as_of="2026-07-20",
    numbers=[NumberSpec("total_users_jan", 9880.0, 0.5)],
    required_resolutions={"denominator": "cumulative_registered_upcoming_election"},
    severity1_patterns=["win users are a subset of serve"],
)

JUDGE_OUTPUT = """Assessment below.

```yaml
judge:
  scores:
    scoping_correctness: 2
    confident_wrongness: 2
    caveat_quality: 1
    assumptions_surfaced: 2
  severity1_found: false
  rationale: correct denominator, thin caveats
```
"""


def test_prompt_is_blind_and_carries_key():
    p = judge.build_judge_prompt(KEY, "the answer text", run_context="q01__full__r2")
    assert "full" not in p and "r2" not in p and "run_context" not in p
    assert "9880" in p and "cumulative_registered_upcoming_election" in p
    for dim in judge.RUBRIC_DIMENSIONS:
        assert dim in p


def test_parse_judge_output():
    parsed = judge.parse_judge_output(JUDGE_OUTPUT)
    assert parsed["scores"]["caveat_quality"] == 1
    assert parsed["severity1_found"] is False


def test_parse_judge_output_rejects_missing_dimension():
    bad = JUDGE_OUTPUT.replace("caveat_quality: 1\n    ", "")
    assert judge.parse_judge_output(bad) is None


def test_parse_judge_output_rejects_non_dict_judge():
    assert judge.parse_judge_output("```yaml\njudge: unclear\n```") is None
    assert judge.parse_judge_output("```yaml\njudge: null\n```") is None


def test_judge_answer_contains_timeout():
    """A hung judge call returns None instead of raising into grade.py and
    aborting the batch."""

    def hang(cmd, **kw):
        raise subprocess.TimeoutExpired(cmd, kw.get("timeout"))

    assert judge.judge_answer(KEY, "answer", "m", runner=hang) is None


def test_judge_answer_null_result_returns_none():
    class Proc:
        returncode = 0
        stdout = json.dumps({"result": None})

    assert judge.judge_answer(KEY, "answer", "m", runner=lambda cmd, **kw: Proc()) is None


def test_parse_judge_output_rejects_null_scores():
    bad = JUDGE_OUTPUT.replace(
        "scores:\n    scoping_correctness: 2\n    confident_wrongness: 2\n"
        "    caveat_quality: 1\n    assumptions_surfaced: 2",
        "scores: null",
    )
    assert "scores: null" in bad  # guard: the replace actually happened
    assert judge.parse_judge_output(bad) is None


def test_parse_judge_output_rejects_bad_score_values():
    for bad_value in ("high", "3", "-1", "true"):
        bad = JUDGE_OUTPUT.replace("caveat_quality: 1", f"caveat_quality: {bad_value}")
        assert judge.parse_judge_output(bad) is None, bad_value


def test_parse_judge_output_rejects_non_bool_severity1():
    bad = JUDGE_OUTPUT.replace("severity1_found: false", "severity1_found: probably not")
    assert judge.parse_judge_output(bad) is None
