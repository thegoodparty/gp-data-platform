# tests/test_candidacy_config.py
"""Tests for the candidacy ER config (DATA-1880 follow-up: office_level comparison)."""

from scripts.configs.candidacy import CANDIDACY_CONFIG, ELECTION_DATE_WINDOW_DAYS


def _election_date_comparison():
    return next(
        c
        for c in CANDIDACY_CONFIG.comparisons
        if c.get_comparison("duckdb").output_column_name == "election_date"
    )


def test_election_date_has_within_window_level():
    """election_date is not exact-only: it carries a within-window level between
    the exact match and ElseLevel, so near-duplicate dates score gamma 1. The
    level thresholds the date difference at ELECTION_DATE_WINDOW_DAYS (in
    seconds) so it agrees with the blocking-rule window."""
    cmp = _election_date_comparison().get_comparison("duckdb").as_dict()
    window_seconds = ELECTION_DATE_WINDOW_DAYS * 86400
    assert any(
        "epoch" in level.get("sql_condition", "").lower()
        and str(window_seconds) in level.get("sql_condition", "")
        for level in cmp["comparison_levels"]
    ), f"Expected a within-{ELECTION_DATE_WINDOW_DAYS}-day (<= {window_seconds}s) level"


def test_election_date_window_sql_is_idempotent():
    """Resolving the comparison repeatedly must not re-wrap the date column.

    AbsoluteDateDifference*Level(input_is_string=True) parses the string via
    try_strptime; a stateful implementation would re-wrap that column on each
    resolution, stacking try_strptime() until the string double-parses to NULL.
    The comparison builder must resolve to stable SQL (one try_strptime per side).
    """
    cmp = _election_date_comparison()
    counts = []
    for _ in range(3):
        d = cmp.get_comparison("duckdb").as_dict()
        sql = next(
            level["sql_condition"]
            for level in d["comparison_levels"]
            if "epoch" in level.get("sql_condition", "").lower()
        )
        counts.append(sql.lower().count("try_strptime"))
    assert counts == [2, 2, 2], f"date-window SQL not idempotent: try_strptime counts {counts}"


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
