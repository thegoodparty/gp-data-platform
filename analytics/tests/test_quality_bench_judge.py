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
