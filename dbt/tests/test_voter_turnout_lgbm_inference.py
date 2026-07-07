"""Unit tests for the pure SQL-building helpers of the nationwide turnout model.

These guard the collapse-specific regressions (row-level state, row-level
opportunity, boolean vote-history) and the routing/detection logic. They import
the helpers directly from the dbt Python model file, which is why `import mlflow`
must stay inside `model()` (the dbt test env has pyspark + pandas, not mlflow).
"""

import numpy as np
import pandas as pd
import pytest
from dbt.project.models.intermediate.l2.int__voter_turnout_lgbm_inference import (
    _OPP_STATES_SQL,
    _SLUG_ELECTION_CODE,
    _assert_consistent_model_family,
    _build_district_membership_sql,
    _build_precinct_features_sql,
    _detect_election_cols,
    _op_years,
    _opp_view_sql,
    _parse_state_allowlist,
    _predict_precinct,
    _read_model_family_tag,
    _select_cat_map_path,
    _year_to_model_slugs,
)

# A representative L2 column set: vote-history (boolean nationwide) + keys + a few features.
# AnyElection is always odd-year, OtherElection is always even-year in the real nationwide
# schema — kept realistic here since the eligibility branch now routes by prefix, not parity.
_L2_COLS = {
    "state_postal_code",
    "County",
    "Precinct",
    "Town_Ward",
    "City_Ward",
    "Town_District",
    "City",
    "Voters_BirthDate",
    "Voters_CalculatedRegDate",
    "Voters_MovedFrom_Date",
    "Voters_Active",
    "FECDonors_NumberOfDonations",
    "ConsumerData_Length_Of_Residence_Code",
    "General_2024",
    "Primary_2024",
    "AnyElection_2023",
    "OtherElection_2024",
    "ConsumerData_Donor_Political_Liberal",  # _Y_INDICATOR (STRING)
}
_ELECTION_COLS = [
    ("General_2024", "General", 2024),
    ("Primary_2024", "Primary", 2024),
    ("AnyElection_2023", "AnyElection", 2023),
    ("OtherElection_2024", "OtherElection", 2024),
]


# ── Task 1: routing + detection ──────────────────────────────────────────────
@pytest.mark.parametrize(
    "year,expected",
    [
        (2026, ["midterm", "even_year_local", "even_year_primary"]),  # even, year % 4 == 2
        (2028, ["presidential_lag3", "even_year_local", "even_year_primary"]),  # even, year % 4 == 0
        (2027, ["off_year_local_lag2"]),  # odd
        (2025, ["off_year_local_lag2"]),  # odd
    ],
)
def test_year_to_model_slugs(year, expected):
    assert _year_to_model_slugs(year) == expected


@pytest.mark.parametrize("year", [2024, 2025, 2026, 2027, 2028, 2029, 2030])
def test_every_routed_slug_has_an_election_code(year):
    # Every slug _year_to_model_slugs can return must have an entry in
    # _SLUG_ELECTION_CODE, or _predict_precinct KeyErrors in production.
    for slug in _year_to_model_slugs(year):
        assert slug in _SLUG_ELECTION_CODE, f"{slug} (routed for {year}) has no election_code mapping"


def test_detect_election_cols_filters_future_years():
    cols = ["General_2024", "General_2026", "AnyElection_2025", "Foo", "Primary_2030"]
    got = _detect_election_cols(cols, max_vote_history_year=2025)
    assert ("General_2024", "General", 2024) in got
    assert ("AnyElection_2025", "AnyElection", 2025) in got
    assert all(year <= 2025 for _, _, year in got)
    assert "Foo" not in [c for c, _, _ in got]


# ── Task 2: precinct-feature SQL builder (collapse fixes) ────────────────────
def test_features_sql_uses_real_state_column_not_lit():
    sql = _build_precinct_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    assert "lit(" not in sql
    assert "state_postal_code" in sql
    assert "AS State" in sql
    assert "GROUP BY state_postal_code, County," in sql


def test_vote_history_uses_boolean_not_y_string():
    sql = _build_precinct_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # the "voted" test must be the boolean column, never "= 'Y'" on a vote-history col.
    assert "`General_2024` = 'Y'" not in sql
    assert "WHEN `General_2024` THEN 1.0" in sql
    # the _Y_INDICATOR (still STRING) keeps its = 'Y' test
    assert "`ConsumerData_Donor_Political_Liberal` = 'Y'" in sql


def test_odd_year_opportunity_is_row_level():
    sql = _build_precinct_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # AnyElection (odd-year local) eligibility branches on the row's state, then the
    # per-precinct opportunity flag — not a Python-resolved scalar.
    assert f"WHEN state_postal_code IN {_OPP_STATES_SQL} THEN 0.0 WHEN opp_2023 = 1 THEN 0.0 ELSE NULL" in sql


def test_even_year_opportunity_has_no_state_shortcut():
    sql = _build_precinct_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # OtherElection (even-year local) eligibility is precinct-opportunity-only — no
    # state-list shortcut, since local election incidence varies precinct-by-precinct.
    assert "WHEN opp_2024 = 1 THEN 0.0 ELSE NULL" in sql
    assert f"state_postal_code IN {_OPP_STATES_SQL} THEN 0.0 WHEN opp_2024" not in sql


def test_opp_years_and_view_sql():
    years = _op_years(_ELECTION_COLS, _L2_COLS, 2026)
    assert years == [2023, 2024]
    view = _opp_view_sql(years, "goodparty_data_catalog", "model_predictions")
    assert "model_predictions.turnout_historical_precincts" in view
    assert "GROUP BY State, County, Precinct" in view
    assert "WHERE State =" not in view  # nationwide: no per-state filter
    assert "opp_2023" in view
    assert "opp_2024" in view


# ── Task 3: allowlist + membership ───────────────────────────────────────────
def test_parse_state_allowlist():
    assert _parse_state_allowlist(None) is None
    assert _parse_state_allowlist("  ") is None
    assert _parse_state_allowlist("al, ny  tx") == {"AL", "NY", "TX"}


def test_membership_sql_is_nationwide_and_majority_rule():
    cols = {
        "state_postal_code",
        "County",
        "Precinct",
        "Town_Ward",
        "City_Ward",
        "Town_District",
        "City",
        "City_Council_Commissioner_District",
    }
    sql = _build_district_membership_sql(cols, ["City_Council_Commissioner_District", "NotAColumn"])
    assert "LATERAL VIEW STACK(1," in sql  # only the valid district col
    assert "n_in * 1.0 / t.total_voters > 0.5" in sql  # majority rule preserved
    assert "GROUP BY state_postal_code, County," in sql  # nationwide grain
    assert "'State' AS district_type, State AS district_name" in sql


def test_membership_sql_raises_without_district_cols():
    with pytest.raises(ValueError):
        _build_district_membership_sql({"state_postal_code"}, ["NotAColumn"])


# ── Hardening (from the diff review): model-family consistency + predict contract ──
def test_assert_consistent_model_family():
    assert _assert_consistent_model_family({"presidential_lag3": "Fam1", "midterm": "Fam1"}) == "Fam1"
    # disagreement must fail loudly rather than mislabel rows with the first slug's value
    with pytest.raises(ValueError):
        _assert_consistent_model_family({"presidential_lag3": "Fam1", "midterm": "Fam2"})


class _FakeBooster:
    """Minimal stand-in for an mlflow.lightgbm booster: feature_name_ + _Booster.predict."""

    def __init__(self, feat_names):
        self.feature_name_ = feat_names
        self._Booster = self

    def predict(self, arr):
        return np.full(arr.shape[0], 0.5)


def test_predict_precinct_encodes_categoricals_and_outputs_contract():
    pdf = pd.DataFrame(
        {
            "State": ["AL", "AL"],
            "County": ["X", "Y"],
            "Precinct": ["1", "2"],
            "n_voters": [10.0, 20.0],
            "age": [40.0, 50.0],
            "Parties_Description": ["Democratic", "Republican"],  # categorical, integer-encoded via cat_map
        }
    )
    cat_map = {"Parties_Description": ["Democratic", "Republican", "Non-Partisan"]}
    booster = _FakeBooster(["age", "Parties_Description"])
    out = _predict_precinct(pdf, booster, cat_map, "midterm", "FamX", 2026)
    assert list(out["p_hat"]) == [0.5, 0.5]
    assert set(out["election_code"]) == {"General"}  # midterm -> General
    assert out["model_family"].iloc[0] == "FamX"
    assert out["inference_year"].iloc[0] == 2026
    assert {"State", "County", "Precinct", "n_voters"}.issubset(out.columns)


def test_select_cat_map_path_returns_single_match():
    only = "/tmp/model/tmpAbC_categorical_feature_map.json"
    assert _select_cat_map_path([only]) == only


@pytest.mark.parametrize(
    "paths",
    [
        [],  # none found -> fail loud (missing artifact)
        [
            "/tmp/model/a_categorical_feature_map.json",
            "/tmp/model/b_categorical_feature_map.json",
        ],  # ambiguous -> fail loud rather than guess a possibly-wrong encoding
    ],
)
def test_select_cat_map_path_raises_unless_exactly_one(paths):
    with pytest.raises(ValueError):
        _select_cat_map_path(paths)


def test_read_model_family_tag_returns_value():
    tags = {
        "model_family": "precinct_level_lgbm_votehistory_socioecondemopolgeo",
        "lightgbm_version": "4.3.0",
    }
    assert (
        _read_model_family_tag(tags, "goodparty_data_catalog.model_predictions.voter_turnout_model_midterm")
        == "precinct_level_lgbm_votehistory_socioecondemopolgeo"
    )


@pytest.mark.parametrize("tags", [None, {}, {"lightgbm_version": "4.3.0"}, {"model_family": ""}])
def test_read_model_family_tag_raises_when_missing_or_empty(tags):
    with pytest.raises(ValueError):
        _read_model_family_tag(tags, "some.model.name")
