# tests/test_person_config.py
"""Tests for the person ER config."""

import re

from scripts.configs.person import PERSON_CONFIG
from scripts.constants import PERSON_POST_PREDICTION_FILTER
from scripts.entity_config import ENTITY_TYPES, get_config


def _comparison_columns() -> set[str]:
    return {c.get_comparison("duckdb").output_column_name for c in PERSON_CONFIG.comparisons}


def _rule_sql(rule) -> str:
    """The rule's compiled DuckDB SQL, whatever kind of rule it is."""
    return rule.create_blocking_rule_dict("duckdb")["blocking_rule"]


def test_person_is_registered():
    assert "person" in ENTITY_TYPES
    assert get_config("person") is PERSON_CONFIG


def test_post_filter_gammas_all_exist():
    """Every gamma_<col> in the filter must name a real comparison.

    predict_and_cluster raises when a filter names a gamma column Splink did not
    emit, so a renamed comparison (birth_date vs birth_year) breaks the run at
    predict time, after EM training has already been paid for.
    """
    referenced = {g.removeprefix("gamma_") for g in re.findall(r"\bgamma_\w+", PERSON_POST_PREDICTION_FILTER)}
    missing = referenced - _comparison_columns()
    assert not missing, f"Filter references gammas with no comparison: {sorted(missing)}"


def test_no_unbounded_blocking_rule():
    """A rule on name alone, or state + last_name alone, is unaffordable.

    The input is ~1M records matched link_and_dedupe, so a common surname would
    generate hundreds of millions of candidate pairs. Every rule has to carry a
    contact key, a first-name restriction, or a birth year.
    """
    narrowing = ("email", "phone", "first_name", "birth_date", "pregroup_id")
    for rule in PERSON_CONFIG.blocking_rules_for_prediction:
        sql = _rule_sql(rule)
        assert any(
            token in sql for token in narrowing
        ), f"Blocking rule has no narrowing term beyond name/state: {sql!r}"
