"""Tests for module-level constants in scripts.constants."""

import duckdb

from scripts.constants import EO_POST_PREDICTION_FILTER, PERSON_POST_PREDICTION_FILTER


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


def _person_row(**overrides):
    """A phone-only pair across unrelated surnames on a line with four groups."""
    base = {
        "gamma_first_name": 3,
        "gamma_last_name": 0,
        "gamma_email": 0,
        "gamma_phone": 1,
        "br_candidate_id_l": None,
        "br_candidate_id_r": None,
        "email_l": "ecarter@cityhall.example.gov",
        "email_r": None,
        "phone_l": "2175550140",
        "phone_r": "2175550140",
        "first_name_l": "elizabeth",
        "first_name_r": "melissa",
        "last_name_tokens_l": ["carter"],
        "last_name_tokens_r": ["dunn"],
        "phone_surname_groups_l": "4",
        "source_name_l": "hubspot",
        "source_name_r": "hubspot",
        "city_l": None,
        "city_r": None,
        "zip5_l": None,
        "zip5_r": None,
    }
    base.update(overrides)
    return base


def test_person_filter_drops_a_phone_only_pair_on_a_shared_line():
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, _person_row()) is False


def test_person_filter_keeps_a_phone_only_pair_on_a_family_line():
    """Three groups and under is a household or small office, not a switchboard."""
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, _person_row(phone_surname_groups_l="3")) is True


def test_person_filter_keeps_a_shared_line_pair_whose_surnames_share_a_token():
    """A middle name packed into the surname field ("ann smith" / "smith")."""
    row = _person_row(last_name_tokens_l=["ann", "smith"], last_name_tokens_r=["smith"])
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, row) is True


def test_person_filter_keeps_a_shared_line_pair_with_a_matching_email():
    row = _person_row(gamma_email=1, email_r="ecarter@cityhall.example.gov")
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, row) is True


def test_person_filter_keeps_a_pair_when_the_surname_groups_are_unknown():
    """A prematch without the column must not start dropping pairs."""
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, _person_row(phone_surname_groups_l=None)) is True


def _product_vs_br(**overrides):
    """A gp_api user against a BallotReady person that agree on name only."""
    base = _person_row(
        gamma_phone=-1,
        phone_l=None,
        phone_r=None,
        email_l=None,
        phone_surname_groups_l=None,
        first_name_l="josh",
        first_name_r="josh",
        source_name_l="gp_api",
        source_name_r="ballotready",
    )
    base.update(overrides)
    return base


def test_person_filter_drops_a_product_record_matched_to_br_on_name_alone():
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, _product_vs_br()) is False


def test_person_filter_drops_a_br_record_matched_to_hubspot_on_name_alone():
    row = _product_vs_br(source_name_l="ballotready", source_name_r="hubspot")
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, row) is False


def test_person_filter_keeps_a_product_record_matched_to_br_on_a_shared_city():
    row = _product_vs_br(city_l="fate", city_r="fate")
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, row) is True


def test_person_filter_keeps_a_product_record_matched_to_br_on_a_shared_zip():
    row = _product_vs_br(zip5_l="75189", zip5_r="75189")
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, row) is True


def test_person_filter_keeps_a_product_record_matched_to_br_on_a_shared_email():
    row = _product_vs_br(email_l="a@b.example", email_r="a@b.example")
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, row) is True


def test_person_filter_leaves_other_source_pairs_alone():
    row = _product_vs_br(source_name_l="techspeed")
    assert _eval_filter(PERSON_POST_PREDICTION_FILTER, row) is True
