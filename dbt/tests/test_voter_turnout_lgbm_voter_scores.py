"""Unit tests for the pure SQL-building helpers of the PER-VOTER turnout model.

The voter model is a precinct-of-1 collapse of the district model, so its helpers
diverge in load-bearing ways: no GROUP BY, row-level eligibility branches, and a
single (slug, election_code) per year instead of a list. These guard that collapse
and the NH/VT opportunity join key. They import the helpers directly from the dbt
Python model file, which is why `import mlflow` must stay inside `model()` (the dbt
test env has pyspark + pandas, not mlflow).
"""

import pytest
from dbt.project.models.intermediate.l2.int__voter_turnout_lgbm_voter_scores import (
    _OPP_STATES_SQL,
    _build_voter_features_sql,
    _detect_election_cols,
    _file_digest,
    _november_model_for_year,
    _op_years,
    _opp_view_sql,
    _parse_state_allowlist,
)

# Mirrors the district test fixture: vote-history (boolean nationwide) + keys + a few
# features. AnyElection is odd-year-only and OtherElection even-year-only in the real
# nationwide schema.
_L2_COLS = {
    "LALVOTERID",
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


# ── routing + detection ──────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "year,expected",
    [
        (2026, ("midterm", "General")),  # even, year % 4 == 2
        (2028, ("presidential_lag3", "General")),  # even, year % 4 == 0
        (2027, ("off_year_local_lag2", "Local_or_Municipal")),  # odd
        (2025, ("off_year_local_lag2", "Local_or_Municipal")),  # odd
    ],
)
def test_november_model_for_year(year, expected):
    # One projection per voter: a single (slug, code), unlike the district model's list.
    assert _november_model_for_year(year) == expected


def test_detect_election_cols_filters_future_years():
    # L2 pre-provisions empty vote-history columns through 2030, so column presence is
    # not data presence: anything past max_vote_history_year must be dropped.
    cols = {"General_2024", "General_2028", "Primary_2024", "NotAnElection"}
    detected = _detect_election_cols(cols, max_vote_history_year=2025)
    assert ("General_2028", "General", 2028) not in detected
    assert ("General_2024", "General", 2024) in detected
    assert all(name != "NotAnElection" for name, _, _ in detected)


# ── the precinct-of-1 collapse ───────────────────────────────────────────────
def test_features_sql_is_row_level_not_aggregated():
    sql = _build_voter_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # The whole point of this model: one row per voter, so no aggregation anywhere.
    assert "GROUP BY" not in sql
    assert "AVG(" not in sql
    assert "COUNT(*)" not in sql
    assert "LALVOTERID" in sql
    # AVG(x) -> x collapses n_voters to the literal 1.
    assert "CAST(1.0 AS DOUBLE) AS n_voters" in sql


def test_vote_history_uses_boolean_not_y_string():
    sql = _build_voter_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # Vote-history columns are BooleanType nationwide; a = 'Y' test would be silently false.
    assert "CASE WHEN `General_2024` THEN 1.0" in sql
    assert "`General_2024` = 'Y'" not in sql
    # Y-indicator consumer columns ARE strings and must still use string equality.
    assert "`ConsumerData_Donor_Political_Liberal` = 'Y'" in sql


def test_odd_year_opportunity_is_row_level():
    sql = _build_voter_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # AnyElection: statewide-odd-year states short-circuit to 0, others fall to the
    # per-precinct flag. Row-level (no aggregate wrapper around opp_).
    assert f"WHEN state_postal_code IN {_OPP_STATES_SQL} THEN 0.0" in sql
    assert "WHEN opp_2023 = 1 THEN 0.0 ELSE NULL" in sql


def test_even_year_opportunity_has_no_state_shortcut():
    sql = _build_voter_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # OtherElection is even-year and has no statewide list: opportunity-flag only.
    assert "WHEN opp_2024 = 1 THEN 0.0 ELSE NULL" in sql


def test_opp_years_and_view_sql():
    years = _op_years(_ELECTION_COLS, _L2_COLS, 2026)
    assert years == [2023, 2024]
    view = _opp_view_sql(years, "goodparty_data_catalog", "model_predictions")
    assert "model_predictions.turnout_historical_precincts" in view
    assert "GROUP BY State, County, Precinct" in view
    assert "WHERE State =" not in view  # nationwide: no per-state filter
    assert "opp_2023" in view
    assert "opp_2024" in view


def test_opp_join_uses_nh_vt_precinct_key():
    sql = _build_voter_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # The _hp_opp join must key precincts the same way the SELECT filter does:
    # NH/VT ward-coalesce, raw Precinct elsewhere. Joining on raw Precinct alone
    # would never match NH/VT (their raw Precinct is mostly NULL and the opportunity
    # table carries ward names), silently zeroing their opp flags and mis-encoding
    # OtherElection lags as NULL instead of 0.
    assert "COALESCE(l2.Town_Ward, l2.City_Ward, l2.Town_District, l2.City)" in sql
    assert "CAST(l2.Precinct AS STRING) = hp.Precinct" not in sql


def test_no_op_years_reads_l2_directly():
    # Only always-held election types in the lag window -> no opportunity years ->
    # the features SQL reads _l2 directly: no _hp_opp join, no opp_ columns.
    cols = {c for c in _L2_COLS if c not in ("AnyElection_2023", "OtherElection_2024")}
    election_cols = [("General_2024", "General", 2024), ("Primary_2024", "Primary", 2024)]
    assert _op_years(election_cols, cols, 2026) == []
    sql = _build_voter_features_sql(cols, election_cols, 2026, 2026)
    assert "FROM _l2 WHERE" in sql
    assert "_hp_opp" not in sql
    assert "opp_" not in sql


def test_features_sql_excludes_voters_without_a_precinct_key():
    sql = _build_voter_features_sql(_L2_COLS, _ELECTION_COLS, 2026, 2026)
    # Voters with no precinct key are unscoreable by the precinct model; the WHERE
    # keeps coverage identical to the precinct path instead of emitting junk rows.
    assert "IS NOT NULL" in sql.split("WHERE")[-1]


# ── misc helpers ─────────────────────────────────────────────────────────────
def test_parse_state_allowlist():
    assert _parse_state_allowlist(None) is None
    assert _parse_state_allowlist("  ") is None
    assert _parse_state_allowlist("al, ny  tx") == {"AL", "NY", "TX"}


def test_file_digest_is_content_addressed(tmp_path):
    # Keys the executor booster cache: identical content must collide, changed
    # content must not, or a reused worker would score with a stale booster.
    a = tmp_path / "a.txt"
    b = tmp_path / "b.txt"
    c = tmp_path / "c.txt"
    a.write_bytes(b"booster-one")
    b.write_bytes(b"booster-one")
    c.write_bytes(b"booster-two")
    assert _file_digest(str(a)) == _file_digest(str(b))
    assert _file_digest(str(a)) != _file_digest(str(c))


def test_file_digest_spans_chunk_boundaries(tmp_path):
    # A small chunk_size must not change the result (the read loop is a real loop).
    p = tmp_path / "big.txt"
    p.write_bytes(b"x" * 5000)
    assert _file_digest(str(p), chunk_size=16) == _file_digest(str(p))
