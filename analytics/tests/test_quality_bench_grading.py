import json
from pathlib import Path

from quality_bench import grading
from quality_bench.bank import Key, NumberSpec

ANSWER = """Here is the analysis.

```yaml
results:
  numbers:
    total_users_jan: 9885
  assumptions:
    - fork: denominator
      resolution: cumulative_registered_upcoming_election
      verified: true
```
"""


def make_key(**kw) -> Key:
    defaults = dict(
        id="q01",
        as_of="2026-07-20",
        numbers=[NumberSpec("total_users_jan", 9880.0, 0.5)],
    )
    defaults.update(kw)
    return Key(**defaults)


def write_transcript(tmp_path: Path, texts: list[str]) -> Path:
    p = tmp_path / "session.jsonl"
    lines = []
    for t in texts:
        lines.append(json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": t}]}}))
    p.write_text("\n".join(lines))
    return p


def test_final_answer_text_takes_last_assistant_message(tmp_path):
    p = write_transcript(tmp_path, ["first", "second"])
    assert grading.final_answer_text(p) == "second"


def test_parse_results_block():
    block = grading.parse_results_block(ANSWER)
    assert block is not None
    assert block["results"]["numbers"]["total_users_jan"] == 9885


def test_parse_results_block_missing_returns_none():
    assert grading.parse_results_block("no yaml here") is None


def test_check_numbers_within_tolerance():
    block = grading.parse_results_block(ANSWER)
    results = grading.check_numbers(block, make_key())
    assert [r.passed for r in results] == [True]  # 9885 vs 9880 is ~0.05%


def test_check_numbers_out_of_tolerance():
    key = make_key(numbers=[NumberSpec("total_users_jan", 9000.0, 0.5)])
    block = grading.parse_results_block(ANSWER)
    results = grading.check_numbers(block, key)
    assert results[0].passed is False
    assert "9885" in results[0].detail


def test_check_numbers_missing_number_fails():
    key = make_key(numbers=[NumberSpec("activated_jan", 100.0, 0.5)])
    block = grading.parse_results_block(ANSWER)
    results = grading.check_numbers(block, key)
    assert results[0].passed is False
    assert "missing" in results[0].detail
