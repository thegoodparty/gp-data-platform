from quality_bench import grading
from quality_bench.bank import Key, NumberSpec, SourceCheck

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


def test_parse_results_block_rejects_non_dict_results():
    """Non-dict results: value should parse to None, not crash."""
    answer = """```yaml
results: not_a_dict
```"""
    block = grading.parse_results_block(answer)
    assert block is None
    # Verify check_numbers doesn't crash on None
    results = grading.check_numbers(block, make_key())
    assert len(results) == 1
    assert results[0].passed is False


def test_parse_results_block_takes_last_fence():
    """Two fences; last one wins. Also covers yml spelling."""
    answer = """```yaml
results:
  numbers:
    total_users_jan: 1111
```

Some text.

```yml
results:
  numbers:
    total_users_jan: 9885
```"""
    block = grading.parse_results_block(answer)
    assert block is not None
    assert block["results"]["numbers"]["total_users_jan"] == 9885


def test_check_numbers_zero_key_value():
    """Zero spec.value should not cause ZeroDivisionError."""
    key = make_key(numbers=[NumberSpec("x", 0.0, 0.5)])
    answer = """```yaml
results:
  numbers:
    x: 5
```"""
    block = grading.parse_results_block(answer)
    results = grading.check_numbers(block, key)
    assert results[0].passed is False  # diff_pct = inf
    assert "inf" in results[0].detail


def test_check_numbers_zero_key_zero_answer_passes():
    """An exact 0 against a 0 key is correct, not an inf diff."""
    key = make_key(numbers=[NumberSpec("x", 0.0, 0.5)])
    block = {"results": {"numbers": {"x": 0}}}
    results = grading.check_numbers(block, key)
    assert results[0].passed is True


def test_cell_consistency_all_zero_reps_consistent():
    """Identical all-zero reps agree perfectly; zero mean must not read as inf spread."""
    key = make_key(numbers=[NumberSpec("x", 0.0, 0.5)])
    cell = grading.cell_consistency([{"results": {"numbers": {"x": 0}}}] * 3, key)
    assert cell["max_spread_pct"] == 0.0
    assert cell["consistent"] is True


def test_check_assumptions_fork_presence():
    key = make_key(required_assumptions=["denominator", "population"])
    block = grading.parse_results_block(ANSWER)  # ledger surfaces denominator only
    by_fork = {c.check_id: c.passed for c in grading.check_assumptions(block, key)}
    assert by_fork == {"denominator": True, "population": False}


def test_check_assumptions_unparsed_block_fails():
    key = make_key(required_assumptions=["denominator"])
    assert grading.check_assumptions(None, key)[0].passed is False


def test_check_assumptions_empty_resolution_fails():
    """A fork listed with an empty or null resolution is not a resolved fork."""
    key = make_key(required_assumptions=["denominator"])
    for resolution in ("''", "null", '"  "'):
        answer = f"""```yaml
results:
  numbers:
    total_users_jan: 9885
  assumptions:
    - fork: denominator
      resolution: {resolution}
```"""
        block = grading.parse_results_block(answer)
        assert grading.check_assumptions(block, key)[0].passed is False, resolution


def test_check_sources_pass_and_fail():
    key = make_key(
        mandatory_sources=[
            SourceCheck("provenance_csv", r"instrumentation_data/.*provenance", "provenance CSV")
        ]
    )
    hit = "ran: Read /omni/instrumentation_data/amplitude_provenance.csv"
    results = grading.check_sources(hit, key)
    assert results[0].passed is True
    results = grading.check_sources("never opened it", key)
    assert results[0].passed is False


def test_check_severity1_match_fails():
    key = make_key(severity1_patterns=[r"win users are a subset of serve"])
    bad = "Note that all Win users are a subset of Serve users, so..."
    results = grading.check_severity1(bad, key)
    assert results[0].passed is False
    ok = grading.check_severity1("Win and Serve overlap partially.", key)
    assert ok[0].passed is True


def block(total: float, resolution: str = "cumulative") -> dict:
    return {
        "results": {
            "numbers": {"total_users_jan": total},
            "assumptions": [{"fork": "denominator", "resolution": resolution, "verified": True}],
        }
    }


def test_check_resolutions_consistently_wrong_is_reported():
    """Sanjay's repro: three reps all resolving the fork the same WRONG way agree
    perfectly (cell consistent) — the wrongness must surface in the reported
    resolutions_match check, run by run."""
    key = make_key(required_resolutions={"denominator": "cumulative_registered"})
    wrong = block(9880, "monthly_active")
    assert grading.check_resolutions(wrong, key)[0].passed is False
    cell = grading.cell_consistency([wrong, wrong, wrong], key)
    assert cell["consistent"] is True  # agreement is correctness-blind by design


def test_check_resolutions_normalized_match():
    key = make_key(required_resolutions={"denominator": "cumulative_registered"})
    paraphrased = block(9880, "Cumulative (registered)")  # punctuation/case differ
    assert grading.check_resolutions(paraphrased, key)[0].passed is True
    assert grading.check_resolutions(None, key)[0].passed is False


def test_cell_consistency_tight_cell():
    key = make_key(required_resolutions={"denominator": "cumulative"})
    cell = grading.cell_consistency([block(9880), block(9885), block(9878)], key)
    assert cell["consistent"] is True
    assert cell["max_spread_pct"] < 0.5
    assert cell["resolution_agreement"]["denominator"] is True


def test_cell_consistency_definition_disagreement():
    key = make_key(required_resolutions={"denominator": "cumulative"})
    cell = grading.cell_consistency([block(9880, "cumulative"), block(5000, "monthly_active")], key)
    assert cell["consistent"] is False
    assert cell["resolution_agreement"]["denominator"] is False


def test_cell_consistency_handles_unparsed_reps():
    key = make_key()
    cell = grading.cell_consistency([block(9880), None, block(9881)], key)
    assert cell["n_reps"] == 3
    assert cell["n_parsed"] == 2


def test_cell_consistency_sparse_number_reports_inf_spread():
    key = make_key(
        numbers=[NumberSpec("total_users_jan", 9880.0, 0.5), NumberSpec("activated_jan", 100.0, 0.5)]
    )
    cell = grading.cell_consistency([block(9880), block(9881)], key)  # activated_jan absent from all reps
    assert cell["consistent"] is False
    assert cell["max_spread_pct"] == float("inf")
