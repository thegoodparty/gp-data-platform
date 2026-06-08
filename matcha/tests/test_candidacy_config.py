# tests/test_candidacy_config.py
"""Tests for the candidacy ER config (DATA-1880 follow-up: office_level comparison)."""

from scripts.configs.candidacy import CANDIDACY_CONFIG


def test_office_level_in_comparisons():
    """office_level is added as an ExactMatch comparison."""
    comparison_columns = [c.get_comparison("duckdb").output_column_name for c in CANDIDACY_CONFIG.comparisons]
    assert "office_level" in comparison_columns


def test_office_level_em_training_block():
    """office_level appears in at least one EM training block (mirrors EO pattern)."""
    assert any(
        "office_level" in cols for cols in CANDIDACY_CONFIG.em_training_blocks
    ), "Expected an EM training block including office_level"


def test_office_level_not_in_additional_columns_to_retain():
    """office_level is a comparison column and should NOT be in additional_columns_to_retain.

    Splink retains comparison columns automatically; listing it would duplicate
    the column and risk SQL errors. Mirrors EO convention.
    """
    retained = set(CANDIDACY_CONFIG.additional_columns_to_retain)
    assert "office_level" not in retained, (
        "office_level is a comparison column and should not be listed in "
        "additional_columns_to_retain — Splink retains comparison columns automatically."
    )


def test_gamma_office_level_in_audit_gamma_columns():
    """gamma_office_level is exposed in the audit gamma columns list."""
    assert "gamma_office_level" in CANDIDACY_CONFIG.audit_gamma_columns


def _first_name_comparison_sql():
    fn = next(
        c
        for c in CANDIDACY_CONFIG.comparisons
        if c.get_comparison("duckdb").output_column_name == "first_name"
    )
    return fn.get_comparison("duckdb").as_dict()


def test_first_name_comparison_has_token_intersect_level():
    """first_name comparison includes an ArrayIntersectLevel over the precomputed
    first_name_tokens column."""
    cmp = _first_name_comparison_sql()
    assert any(
        "first_name_tokens" in level.get("sql_condition", "") for level in cmp["comparison_levels"]
    ), "Expected an ArrayIntersectLevel over first_name_tokens"
