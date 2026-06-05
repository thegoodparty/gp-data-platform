# tests/test_pipeline.py
"""Tests for pipeline.load_and_prepare."""

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from scripts.configs.candidacy import CANDIDACY_CONFIG
from scripts.configs.elected_official import ELECTED_OFFICIAL_CONFIG
from scripts.pipeline import build_settings, load_and_prepare, run


def _make_input(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal prematch DataFrame."""
    defaults = {
        "unique_id": "id_0",
        "source_name": "src_a",
        "first_name": "jane",
        "last_name": "doe",
        "first_name_aliases": '["jane"]',
        "election_date": "2024-11-05",
        "party": None,
        "email": None,
        "phone": None,
        "state": "WI",
        "official_office_name": "city council",
        "district_identifier": None,
        "br_race_id": None,
    }
    full_rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(full_rows).astype(str)


def test_load_and_prepare_multi_source():
    """N distinct source_name values -> N DataFrames, sorted by name."""
    df = _make_input(
        [
            {"unique_id": "1", "source_name": "charlie"},
            {"unique_id": "2", "source_name": "alpha"},
            {"unique_id": "3", "source_name": "bravo"},
            {"unique_id": "4", "source_name": "alpha"},
        ]
    )
    result = load_and_prepare(df, CANDIDACY_CONFIG)
    assert len(result) == 3
    assert list(result[0]["source_name"].unique()) == ["alpha"]
    assert list(result[1]["source_name"].unique()) == ["bravo"]
    assert list(result[2]["source_name"].unique()) == ["charlie"]
    assert len(result[0]) == 2  # alpha has 2 records


def test_load_and_prepare_null_normalization():
    """Empty strings, 'nan', 'null' all become None."""
    df = _make_input(
        [
            {"unique_id": "1", "source_name": "a", "email": ""},
            {"unique_id": "2", "source_name": "a", "email": "nan"},
            {"unique_id": "3", "source_name": "a", "email": "null"},
            {"unique_id": "4", "source_name": "b", "email": "real@test.com"},
        ]
    )
    result = load_and_prepare(df, CANDIDACY_CONFIG)
    a_df = result[0]  # source "a"
    assert a_df["email"].isna().all() or (a_df["email"] == None).all()  # noqa: E711
    b_df = result[1]  # source "b"
    assert b_df["email"].iloc[0] == "real@test.com"


def test_load_and_prepare_aliases_parsed():
    """JSON alias strings are parsed into Python lists."""
    df = _make_input(
        [
            {
                "unique_id": "1",
                "source_name": "a",
                "first_name_aliases": '["robert", "bob", "rob"]',
            },
            {
                "unique_id": "2",
                "source_name": "b",
                "first_name_aliases": '["jane"]',
            },
        ]
    )
    result = load_and_prepare(df, CANDIDACY_CONFIG)
    aliases_a = result[0]["first_name_aliases"].iloc[0]
    assert isinstance(aliases_a, list)
    assert "bob" in aliases_a
    assert len(aliases_a) == 3


# ── Elected Officials tests ──


def _make_eo_input(rows: list[dict]) -> pd.DataFrame:
    """Build a minimal elected officials prematch DataFrame."""
    defaults = {
        "unique_id": "id_0",
        "source_name": "src_a",
        "first_name": "jane",
        "last_name": "doe",
        "first_name_aliases": '["jane"]',
        "party": None,
        "email": None,
        "phone": None,
        "state": "WI",
        "official_office_name": "city council",
        "district_identifier": None,
        "office_type": "City Council",
        "office_level": "Local",
        "city": None,
        "candidate_office": "City Council",
    }
    full_rows = [{**defaults, **r} for r in rows]
    return pd.DataFrame(full_rows).astype(str)


def test_load_and_prepare_eo_no_date_parsing():
    """Elected officials config skips date parsing (no date_columns)."""
    df = _make_eo_input(
        [
            {"unique_id": "1", "source_name": "ballotready"},
            {"unique_id": "2", "source_name": "techspeed"},
        ]
    )
    result = load_and_prepare(df, ELECTED_OFFICIAL_CONFIG)
    assert len(result) == 2
    assert list(result[0]["source_name"].unique()) == ["ballotready"]
    # No election_date column should be created or parsed
    assert "election_date" not in result[0].columns


def test_build_settings_candidacy():
    """build_settings wires the correct number of comparisons and blocking rules."""
    settings = build_settings(CANDIDACY_CONFIG)
    assert len(settings.comparisons) == 10


def test_build_settings_elected_official():
    """EO build_settings has 11 comparisons (no election_date, adds office_type + office_level + ballotready_position_id)."""
    settings = build_settings(ELECTED_OFFICIAL_CONFIG)
    assert len(settings.comparisons) == 11


# ── E2E Smoke Test ──


def test_eo_pipeline_smoke(tmp_path):
    """Full EO pipeline on tiny fixture: proves comparisons, blocking, filters, clustering work."""
    df = pd.read_csv(Path(__file__).parent / "dummy_data_elected.csv", dtype=str)
    pairwise_df, clustered_df = run(
        input_df=df, output_dir=tmp_path, config=ELECTED_OFFICIAL_CONFIG
    )

    # Pipeline completed without error
    assert len(pairwise_df) > 0, "No pairwise predictions generated"
    assert len(clustered_df) > 0, "No clustered records generated"

    # Output files written
    assert (tmp_path / "pairwise_predictions.csv").exists()
    assert (tmp_path / "clustered_elected_officials.csv").exists()

    # At least 1 cross-source cluster (proves matching worked)
    multi_source = (
        clustered_df.groupby("cluster_id")["source_dataset"].nunique() > 1
    ).sum()
    assert multi_source >= 1, f"Expected cross-source clusters, got {multi_source}"

    # EO-specific retained columns present in clustered output
    for col in ["source_name", "office_type", "office_level"]:
        assert col in clustered_df.columns, f"Missing retained column: {col}"


def test_eo_pipeline_smoke_synonym_match(tmp_path):
    """br_006 (City Alderperson) and ts_004 (Springfield City Council) must cluster together."""
    df = pd.read_csv(Path(__file__).parent / "dummy_data_elected.csv", dtype=str)
    pairwise_df, clustered_df = run(
        input_df=df, output_dir=tmp_path, config=ELECTED_OFFICIAL_CONFIG
    )

    br_006_cluster = clustered_df.loc[
        clustered_df["unique_id"] == "br_006", "cluster_id"
    ]
    assert len(br_006_cluster) == 1, "br_006 not found in clustered output"

    ts_004_cluster = clustered_df.loc[
        clustered_df["unique_id"] == "ts_004", "cluster_id"
    ]
    assert len(ts_004_cluster) == 1, "ts_004 not found in clustered output"
    assert (
        br_006_cluster.iloc[0] == ts_004_cluster.iloc[0]
    ), "br_006 and ts_004 should be in the same cluster (City Alderperson == City Council)"


def test_eo_pipeline_smoke_rejects_cross_office_same_name(tmp_path):
    """Same name + same state but different office_type and no contact match must not cluster."""
    df = pd.read_csv(Path(__file__).parent / "dummy_data_elected.csv", dtype=str)
    _, clustered_df = run(
        input_df=df, output_dir=tmp_path, config=ELECTED_OFFICIAL_CONFIG
    )

    br_010_cluster = clustered_df.loc[
        clustered_df["unique_id"] == "br_010", "cluster_id"
    ]
    ts_005_cluster = clustered_df.loc[
        clustered_df["unique_id"] == "ts_005", "cluster_id"
    ]
    assert len(br_010_cluster) == 1, "br_010 not found in clustered output"
    assert len(ts_005_cluster) == 1, "ts_005 not found in clustered output"
    assert (
        br_010_cluster.iloc[0] != ts_005_cluster.iloc[0]
    ), "James Wilson (School Board) and James Wilson (City Council) should NOT cluster"


def test_eo_pipeline_smoke_token_overlap_preserved(tmp_path):
    """Pairs rescued by locality-token overlap (no contact, no office_type match) must still match."""
    df = pd.read_csv(Path(__file__).parent / "dummy_data_elected.csv", dtype=str)
    pairwise_df, _ = run(
        input_df=df, output_dir=tmp_path, config=ELECTED_OFFICIAL_CONFIG
    )

    # br_012/ts_010: "hamilton county: springdale township trustee" vs "springdale village council"
    # JW < 0.75, different office_type, no email/phone — rescued by shared "springdale" token
    pair_exists = (
        (pairwise_df["unique_id_l"] == "br_012")
        & (pairwise_df["unique_id_r"] == "ts_010")
    ).any() or (
        (pairwise_df["unique_id_l"] == "ts_010")
        & (pairwise_df["unique_id_r"] == "br_012")
    ).any()
    assert pair_exists, "br_012/ts_010 pair should survive via locality-token overlap"


def test_eo_pipeline_smoke_contact_bypass(tmp_path):
    """Contact-confirmed pairs bypass office compatibility — survive post-prediction filter."""
    df = pd.read_csv(Path(__file__).parent / "dummy_data_elected.csv", dtype=str)
    pairwise_df, _ = run(
        input_df=df, output_dir=tmp_path, config=ELECTED_OFFICIAL_CONFIG
    )

    # br_005 (Linda Brown, County Clerk, office_type=County) and ts_011 (Linda Brown,
    # County Board Supervisor, office_type=County Board) share phone but have different
    # office_type, different office title, and no locality-token overlap.
    # The pair must survive post-prediction filtering via gamma_phone > 0 bypass.
    # (On tiny fixture data the match probability is too low to cluster, but the
    # filter must not be the reason it fails — that's what this test proves.)
    pair_exists = (
        (pairwise_df["unique_id_l"] == "br_005")
        & (pairwise_df["unique_id_r"] == "ts_011")
    ).any() or (
        (pairwise_df["unique_id_l"] == "ts_011")
        & (pairwise_df["unique_id_r"] == "br_005")
    ).any()
    assert pair_exists, "br_005/ts_011 pair should survive filter via phone bypass"


def test_train_model_fails_when_all_blocks_fail():
    """train_model raises RuntimeError if every EM block fails."""
    from scripts.pipeline import train_model

    mock_linker = MagicMock()
    mock_linker.training.estimate_parameters_using_expectation_maximisation.side_effect = RuntimeError(
        "EM failed"
    )

    with pytest.raises(RuntimeError, match="EM training failed for all"):
        train_model(mock_linker, CANDIDACY_CONFIG)


def test_train_model_continues_on_partial_failure(capsys):
    """train_model warns but continues if some (not all) EM blocks fail."""
    from scripts.pipeline import train_model

    mock_linker = MagicMock()
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("EM failed for first block")

    mock_linker.training.estimate_parameters_using_expectation_maximisation.side_effect = (
        side_effect
    )

    result = train_model(mock_linker, CANDIDACY_CONFIG)

    assert result == len(CANDIDACY_CONFIG.em_training_blocks) - 1
    captured = capsys.readouterr()
    assert "WARNING" in captured.out
    assert "EM training blocks succeeded:" in captured.out


def test_train_model_returns_full_count_on_success():
    """train_model returns the full count when all EM blocks succeed."""
    from scripts.pipeline import train_model

    mock_linker = MagicMock()
    result = train_model(mock_linker, CANDIDACY_CONFIG)
    assert result == len(CANDIDACY_CONFIG.em_training_blocks)


def test_eo_pipeline_writes_filtered_pairs(tmp_path):
    """Pipeline writes filtered_pairs.csv with pairs removed by post-prediction filters."""
    df = pd.read_csv(Path(__file__).parent / "dummy_data_elected.csv", dtype=str)
    run(input_df=df, output_dir=tmp_path, config=ELECTED_OFFICIAL_CONFIG)

    filtered_path = tmp_path / "filtered_pairs.csv"
    assert filtered_path.exists(), "filtered_pairs.csv should be written"

    filtered_df = pd.read_csv(filtered_path)
    assert "unique_id_l" in filtered_df.columns
    assert "unique_id_r" in filtered_df.columns
    assert "match_probability_pre_filter" in filtered_df.columns
    assert "match_weight_pre_filter" in filtered_df.columns

    # Pair keys should be canonicalized (unique_id_l < unique_id_r)
    for _, row in filtered_df.iterrows():
        assert (
            row["unique_id_l"] <= row["unique_id_r"]
        ), f"Pair keys not canonicalized: {row['unique_id_l']} > {row['unique_id_r']}"
