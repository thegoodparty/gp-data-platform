"""Tests for the election_stage post-prediction filter constant."""

import duckdb

from scripts.constants import ELECTION_STAGE_POST_PREDICTION_FILTER

# Columns the election_stage filter references, as DuckDB SQL expressions for a
# single synthetic pair. Defaults: same state/date/stage/office, both office
# names all stop words ("board of education" -> no locality tokens), no
# district/seat conflict, and gamma below the near-exact tier so the office
# clause must be satisfied by candidate_office + locality (not the JW bypass).
_BASE_PAIR = {
    "state_l": "'WI'",
    "state_r": "'WI'",
    "election_date_l": "'2026-04-07'",
    "election_date_r": "'2026-04-07'",
    "election_stage_l": "'general'",
    "election_stage_r": "'general'",
    "gamma_official_office_name": "1",
    "candidate_office_l": "'School Board'",
    "candidate_office_r": "'School Board'",
    "official_office_name_l": "'board of education'",
    "official_office_name_r": "'board of education'",
    "district_identifier_l": "cast(null as integer)",
    "district_identifier_r": "cast(null as integer)",
    "seat_name_l": "cast(null as varchar)",
    "seat_name_r": "cast(null as varchar)",
    # br_race_id anchor: NULL by default so the anchor bypass never fires for
    # the office-identity tests below (they exercise the office-name path).
    "br_race_id_l": "cast(null as integer)",
    "br_race_id_r": "cast(null as integer)",
    # candidacy-overlap anchor: empty by default so it never fires unless a test
    # sets overlapping clusters.
    "matched_candidacy_stage_clusters_l": "cast([] as varchar[])",
    "matched_candidacy_stage_clusters_r": "cast([] as varchar[])",
}


def _pair_passes_filter(**overrides):
    """Evaluate ELECTION_STAGE_POST_PREDICTION_FILTER over one synthetic pair in
    DuckDB (the engine matcha runs on). Returns True if the pair survives."""
    cols = {**_BASE_PAIR, **overrides}
    select = ", ".join(f"{expr} as {name}" for name, expr in cols.items())
    con = duckdb.connect()
    try:
        (n,) = con.execute(
            f"select count(*) from (select {select}) " f"where {ELECTION_STAGE_POST_PREDICTION_FILTER}"
        ).fetchone()
    finally:
        con.close()
    return n == 1


def test_zero_locality_tokens_same_office_passes():
    """Both office names reduce to no locality tokens ("board of education").
    With the same candidate_office and no district/seat conflict there is no
    locality disagreement, so the pair must pass."""
    assert _pair_passes_filter() is True


def test_zero_locality_tokens_different_office_rejected():
    """Same all-stop-word office names but different candidate_office must be
    rejected -- the zero-token bypass only applies when candidate_office matches."""
    assert _pair_passes_filter(candidate_office_r="'County Commission'") is False


def test_different_locality_tokens_rejected():
    """Set-equality regression guard: same candidate_office but distinct locality
    tokens ("nelson" vs "suamico") must not merge."""
    assert (
        _pair_passes_filter(
            official_office_name_l="'nelson village president'",
            official_office_name_r="'suamico village president'",
        )
        is False
    )


def test_asymmetric_locality_tokens_rejected():
    """Single-token office must not match a strictly longer token list sharing
    that token. {'grand'} vs {'grand','prairie'} are different localities, so
    set equality (not subset) must reject the pair."""
    assert (
        _pair_passes_filter(
            official_office_name_l="'grand village president'",
            official_office_name_r="'grand prairie village president'",
        )
        is False
    )


def test_br_race_id_anchor_bypasses_office_name_mismatch():
    """A shared br_race_id (TS's own BR reference) stands in for office-name
    agreement: even with a different candidate_office and a non-matching office
    name, the pair passes when state/date/stage agree."""
    assert (
        _pair_passes_filter(
            br_race_id_l="12345",
            br_race_id_r="12345",
            candidate_office_l="'School Board'",
            candidate_office_r="'City Council'",
            official_office_name_l="'lincoln county r-iv school board'",
            official_office_name_r="'winfield r-4 school district'",
        )
        is True
    )


def test_br_race_id_anchor_still_rejects_stage_mismatch():
    """The anchor bypasses office identity but NOT the stage gate: a shared
    br_race_id that maps to the wrong BR stage (primary vs general) is rejected."""
    assert (
        _pair_passes_filter(
            br_race_id_l="12345",
            br_race_id_r="12345",
            election_stage_l="'primary'",
            election_stage_r="'general'",
        )
        is False
    )


def test_candidacy_cluster_overlap_bypasses_office_name_mismatch():
    """A shared matched candidacy_stage cluster (a candidate in common) stands
    in for office-name agreement: different candidate_office and office name
    still pass when the clusters overlap and state/date/stage agree."""
    assert (
        _pair_passes_filter(
            matched_candidacy_stage_clusters_l="['c1', 'c2']",
            matched_candidacy_stage_clusters_r="['c2', 'c3']",
            candidate_office_l="'School Board'",
            candidate_office_r="'City Council'",
            official_office_name_l="'lincoln county r-iv school board'",
            official_office_name_r="'winfield r-4 school district'",
        )
        is True
    )


def test_disjoint_candidacy_clusters_do_not_bypass_office():
    """Non-overlapping candidacy clusters carry no shared candidate, so they
    must not satisfy office identity on their own."""
    assert (
        _pair_passes_filter(
            matched_candidacy_stage_clusters_l="['c1', 'c2']",
            matched_candidacy_stage_clusters_r="['c3', 'c4']",
            candidate_office_l="'School Board'",
            candidate_office_r="'City Council'",
            official_office_name_l="'springfield school board'",
            official_office_name_r="'madison city council'",
        )
        is False
    )


def test_null_br_race_id_does_not_bypass_office():
    """NULL br_race_id (DDHQ, BR<->BR) must not satisfy office identity: with no
    office-name or candidate_office/locality agreement the pair is rejected."""
    assert (
        _pair_passes_filter(
            candidate_office_l="'School Board'",
            candidate_office_r="'City Council'",
            official_office_name_l="'springfield school board'",
            official_office_name_r="'madison city council'",
        )
        is False
    )


def test_filter_requires_state_date_and_stage():
    """Filter must require matching state, election_date, and election_stage.

    These are the non-office identity keys. state and election_date are
    exact-equality blocking keys, so Splink never trains their m and drops the
    gamma_<col>; the filter references the retained raw _l/_r columns instead.
    """
    assert "state_l = state_r" in ELECTION_STAGE_POST_PREDICTION_FILTER
    assert "election_date_l = election_date_r" in ELECTION_STAGE_POST_PREDICTION_FILTER
    assert "election_stage_l = election_stage_r" in ELECTION_STAGE_POST_PREDICTION_FILTER
    # These gamma columns are dropped by Splink, so must not be referenced.
    assert "gamma_state" not in ELECTION_STAGE_POST_PREDICTION_FILTER
    assert "gamma_election_date" not in ELECTION_STAGE_POST_PREDICTION_FILTER


def test_filter_requires_district_and_seat_agreement():
    """When both sides expose district_identifier/seat_name they must agree."""
    assert "district_identifier_l = district_identifier_r" in ELECTION_STAGE_POST_PREDICTION_FILTER
    assert "seat_name_l = seat_name_r" in ELECTION_STAGE_POST_PREDICTION_FILTER


def test_filter_requires_office_type_with_locality():
    """Office match requires same candidate_office OR near-exact full name."""
    assert "candidate_office_l = candidate_office_r" in ELECTION_STAGE_POST_PREDICTION_FILTER


def test_filter_requires_office_overlap():
    """Filter must require office name overlap or normalized office match."""
    assert "gamma_official_office_name" in ELECTION_STAGE_POST_PREDICTION_FILTER


def test_filter_does_not_require_person_signals():
    """Race-level filter should not reference person columns."""
    assert "gamma_last_name" not in ELECTION_STAGE_POST_PREDICTION_FILTER
    assert "gamma_first_name" not in ELECTION_STAGE_POST_PREDICTION_FILTER
    assert "gamma_email" not in ELECTION_STAGE_POST_PREDICTION_FILTER
