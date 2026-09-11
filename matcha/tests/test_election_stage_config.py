"""Tests for the election_stage ER config (DATA-1582)."""

from scripts.configs.election_stage import ELECTION_STAGE_CONFIG


def test_entity_type():
    assert ELECTION_STAGE_CONFIG.entity_type == "election_stage"


def test_comparisons_include_required_signals():
    """Election-stage matching needs office + date + geo + special signals.

    ballotready_position_id is deliberately absent: the prematch model populates
    it on BallotReady rows only, and this is a link_only job, so no cross-source
    pair can ever agree on it. seat_name used to be in the same position and is
    now compared, because the prematch model parses it out of
    official_office_name for every source.
    """
    comparison_columns = [
        c.get_comparison("duckdb").output_column_name for c in ELECTION_STAGE_CONFIG.comparisons
    ]
    for required in (
        "state",
        "official_office_name",
        "election_date",
        "election_stage",
        "is_special",
        "district_identifier",
        "office_level",
        "office_type",
        "candidate_office",
        "seat_name",
    ):
        assert required in comparison_columns, f"missing comparison: {required}"
    assert "ballotready_position_id" not in comparison_columns


def test_em_blocks_avoid_ballotready_only_keys():
    """A training block requiring agreement on a BallotReady-only column yields
    zero pairs in a link_only job, so EM training on it fails outright. This is
    what broke election_stage. seat_name is no longer in this set, because the
    prematch model parses it for every source, but it stays out of the blocks:
    at ~9% coverage it would generate too few pairs to train on.
    """
    ballotready_only = {"ballotready_position_id"}
    for cols in ELECTION_STAGE_CONFIG.em_training_blocks:
        assert not (set(cols) & ballotready_only), cols


def test_cluster_threshold_is_the_measured_operating_point():
    """0.94 rather than the 0.95 default. Dropping the BallotReady-only
    comparisons and bounding the third EM block shifted scores up, moving the
    operating point: measured on 775,742 records, 0.94 reproduces every
    cross-source pair 0.95 produced beforehand and adds 818, with no growth in
    the largest cluster.
    """
    assert ELECTION_STAGE_CONFIG.cluster_threshold == 0.94


def test_no_person_level_comparisons():
    """Race-level matcher must not include person fields."""
    comparison_columns = [
        c.get_comparison("duckdb").output_column_name for c in ELECTION_STAGE_CONFIG.comparisons
    ]
    for forbidden in ("first_name", "last_name", "party", "email", "phone"):
        assert forbidden not in comparison_columns, (
            f"comparison '{forbidden}' is person-level and should not appear " f"in election_stage config"
        )


def test_em_training_blocks_use_state():
    """Every EM training block should include state to keep blocks tractable."""
    for cols in ELECTION_STAGE_CONFIG.em_training_blocks:
        assert "state" in cols, f"EM block {cols} missing state"


def test_date_columns():
    assert "election_date" in ELECTION_STAGE_CONFIG.date_columns


def test_post_prediction_filters_include_election_stage_filter():
    """Config must include the race-level filter (no person signals)."""
    from scripts.constants import ELECTION_STAGE_POST_PREDICTION_FILTER

    assert ELECTION_STAGE_POST_PREDICTION_FILTER in ELECTION_STAGE_CONFIG.post_prediction_filters


def test_default_input_table():
    assert ELECTION_STAGE_CONFIG.default_input_table == (
        "goodparty_data_catalog.dbt.int__er_prematch_election_stages"
    )


def test_clustered_output_name():
    assert ELECTION_STAGE_CONFIG.clustered_output_name == "clustered_election_stages.csv"
