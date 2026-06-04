"""Tests for module-level constants in scripts.constants."""

import duckdb

from scripts.constants import EO_POST_PREDICTION_FILTER


def test_eo_post_filter_includes_position_id_clause():
    # The third clause should accept a pair when ballotready_position_id matches,
    # so a strong position match isn't filtered out by office_name/office_type.
    assert (
        "gamma_ballotready_position_id" in EO_POST_PREDICTION_FILTER
    ), "EO_POST_PREDICTION_FILTER missing gamma_ballotready_position_id clause"


def test_eo_filter_guards_locality_branch_with_district_identifier():
    """Locality-token-overlap branch must be guarded by a district_identifier check.

    Same-locality records with disagreeing district_identifiers (e.g. city council
    district 5 vs county board district 8) are different races and should not be
    rescued by shared city/county tokens alone. The BASE filter no longer carries
    this branch (office overlap there is handled by the normalized-token
    CustomComparison), so the guard is exercised against the EO filter, which still
    relies on the inline locality-token fallback.
    """
    assert "district_identifier_l" in EO_POST_PREDICTION_FILTER
    assert "district_identifier_r" in EO_POST_PREDICTION_FILTER


def _eval_filter(filter_sql: str, row: dict) -> bool:
    """Evaluate a post-prediction filter SQL expression against a single row."""
    con = duckdb.connect()
    placeholders = ", ".join(f"? AS {k}" for k in row.keys())
    sql = f"SELECT ({filter_sql}) AS keep FROM (SELECT {placeholders})"
    return con.execute(sql, list(row.values())).fetchone()[0]


def _row(**overrides):
    """Build a row dict with all columns the EO filter references."""
    base = {
        "gamma_last_name": 3,
        "gamma_first_name": 3,
        "gamma_email": -1,
        "gamma_phone": -1,
        "gamma_official_office_name": 0,
        "gamma_office_type": 0,
        "gamma_ballotready_position_id": 0,
        "official_office_name_l": "racine city council - district 14",
        "official_office_name_r": "racine county board of supervisors - district 8",
        "district_identifier_l": None,
        "district_identifier_r": None,
    }
    base.update(overrides)
    return base


def test_eo_filter_drops_same_locality_different_district():
    """Different district_identifiers + only locality-token overlap → filtered out."""
    row = _row(district_identifier_l="14", district_identifier_r="8")
    assert _eval_filter(EO_POST_PREDICTION_FILTER, row) is False


def test_eo_filter_keeps_same_locality_matching_district():
    """Same district_identifier + locality-token overlap → allowed."""
    row = _row(district_identifier_l="8", district_identifier_r="8")
    assert _eval_filter(EO_POST_PREDICTION_FILTER, row) is True


def test_eo_filter_keeps_same_locality_one_null_district():
    """One side null district_identifier (e.g. ddhq) + locality overlap → allowed."""
    row = _row(district_identifier_l=None, district_identifier_r="8")
    assert _eval_filter(EO_POST_PREDICTION_FILTER, row) is True


def test_eo_filter_keeps_strong_office_name_match_despite_district_diff():
    """High office-name JW (gamma >= 1) bypasses the district_identifier guard."""
    row = _row(
        gamma_official_office_name=2,
        district_identifier_l="14",
        district_identifier_r="8",
    )
    assert _eval_filter(EO_POST_PREDICTION_FILTER, row) is True
