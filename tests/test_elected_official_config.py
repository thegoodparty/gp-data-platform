# tests/test_elected_official_config.py
"""Tests for the elected_official ER config (DATA-1731 prematch rewrite)."""

from scripts.configs.elected_official import ELECTED_OFFICIAL_CONFIG


def test_ballotready_position_id_in_comparisons():
    """ballotready_position_id is added as an ExactMatch comparison."""
    comparison_columns = [
        c.get_comparison("duckdb").output_column_name
        for c in ELECTED_OFFICIAL_CONFIG.comparisons
    ]
    assert "ballotready_position_id" in comparison_columns


def _rule_text(rule) -> str:
    """Extract a searchable representation of a Splink blocking rule.

    Handles CustomRule (sql_condition), ExactMatchRule (col_expression),
    and And (blocking_rules list of nested rules).
    """
    if hasattr(rule, "sql_condition"):
        return rule.sql_condition or ""
    if hasattr(rule, "col_expression"):
        return getattr(rule.col_expression, "sql_expression", "") or ""
    if hasattr(rule, "blocking_rules"):
        return " AND ".join(_rule_text(r) for r in rule.blocking_rules)
    return ""


def test_ballotready_position_id_in_blocking_rules():
    """A blocking rule references ballotready_position_id (state + position_id)."""
    blocking_rule_strs = [
        _rule_text(r) for r in ELECTED_OFFICIAL_CONFIG.blocking_rules_for_prediction
    ]
    assert any(
        "ballotready_position_id" in s for s in blocking_rule_strs
    ), "Expected a blocking rule referencing ballotready_position_id"


def test_new_columns_in_additional_columns_to_retain():
    """The 4 new retained columns are present (gp_api product IDs only).

    Note: ballotready_position_id is intentionally NOT in additional_columns_to_retain
    because it's a comparison column (cl.ExactMatch at line 47) — Splink retains
    comparison columns automatically. Same convention as office_level and office_type.

    Note: ICP flags (is_win_icp, is_serve_icp, is_win_supersize_icp) are also NOT
    in additional_columns_to_retain — they're functional attributes from
    int__civics_elected_official_ballotready, not match-process columns or
    identifiers. The ER pipeline retains only matching-process telemetry plus
    passthrough identifiers; functional attributes belong on the source intermediate.
    """
    expected = {
        "gp_api_user_id",
        "gp_api_campaign_id",
        "gp_api_elected_office_id",
        "gp_api_organization_slug",
    }
    retained = set(ELECTED_OFFICIAL_CONFIG.additional_columns_to_retain)
    missing = expected - retained
    assert not missing, f"Missing retained columns: {missing}"
    # Negative assertions: comparison columns and functional attributes should NOT
    # be in additional_columns_to_retain
    assert "ballotready_position_id" not in retained, (
        "ballotready_position_id is a comparison column and should not be listed in "
        "additional_columns_to_retain — Splink retains comparison columns automatically."
    )
    for icp_col in ("is_win_icp", "is_serve_icp", "is_win_supersize_icp"):
        assert icp_col not in retained, (
            f"{icp_col} is a functional attribute (not a match-process column or "
            "identifier) and should not be listed in additional_columns_to_retain. "
            "Downstream consumers fetch ICP from int__civics_elected_official_ballotready "
            "via br_office_holder_id join."
        )


def test_first_name_comparison_has_token_intersect_level():
    """first_name comparison mirrors candidacy: includes an ArrayIntersectLevel
    over the precomputed first_name_tokens column so compound names overlap on a
    shared >=2-char token (normalization/tokenization happens upstream in dbt)."""
    fn = next(
        c
        for c in ELECTED_OFFICIAL_CONFIG.comparisons
        if c.get_comparison("duckdb").output_column_name == "first_name"
    )
    assert any(
        "first_name_tokens" in level.get("sql_condition", "")
        for level in fn.get_comparison("duckdb").as_dict()["comparison_levels"]
    ), "Expected an ArrayIntersectLevel over first_name_tokens"
