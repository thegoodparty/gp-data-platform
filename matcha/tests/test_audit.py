# tests/test_audit.py
"""Tests for audit false-negative 3-state classification."""

import pandas as pd

from scripts.audit_false_negatives import (
    _canonicalize_pair_key,
    _classify_pair,
    run_false_negatives,
)
from scripts.audit_low_confidence import run_low_confidence
from scripts.audit_summary import run_summary
from scripts.configs.candidacy import CANDIDACY_CONFIG


def test_canonicalize_pair_key():
    """Pair keys are sorted so id_l < id_r."""
    assert _canonicalize_pair_key("b", "a") == ("a", "b")
    assert _canonicalize_pair_key("a", "b") == ("a", "b")
    assert _canonicalize_pair_key("x", "x") == ("x", "x")


def test_classify_generated_and_kept():
    """Pair in pairwise_predictions -> generated_and_kept."""
    pairwise_keys = {("a", "b")}
    filtered_keys = set()
    assert _classify_pair("a", "b", pairwise_keys, filtered_keys) == "generated_and_kept"
    assert _classify_pair("b", "a", pairwise_keys, filtered_keys) == "generated_and_kept"


def test_classify_generated_but_filtered():
    """Pair in filtered_pairs but not pairwise -> generated_but_filtered."""
    pairwise_keys = set()
    filtered_keys = {("a", "b")}
    assert _classify_pair("a", "b", pairwise_keys, filtered_keys) == "generated_but_filtered"


def test_classify_never_generated():
    """Pair in neither file -> never_generated."""
    pairwise_keys = set()
    filtered_keys = set()
    assert _classify_pair("a", "b", pairwise_keys, filtered_keys) == "never_generated"


def test_classify_legacy_fallback_no_filtered_file():
    """When filtered_pairs.csv is missing, only 2 states are available."""
    pairwise_keys = {("a", "b")}
    assert _classify_pair("a", "b", pairwise_keys, None) == "generated_and_kept"
    assert _classify_pair("c", "d", pairwise_keys, None) == "never_generated"


# --- audit_summary ------------------------------------------------------------


def _summary_frames():
    input_df = pd.DataFrame(
        {
            "unique_id": ["b1", "b2", "t1", "t2"],
            "source_name": ["ballotready", "ballotready", "techspeed", "techspeed"],
        }
    )
    pairwise_df = pd.DataFrame(
        {
            "unique_id_l": ["b1"],
            "unique_id_r": ["t1"],
            "source_name_l": ["ballotready"],
            "source_name_r": ["techspeed"],
            "match_probability": [0.97],
        }
    )
    clustered_df = pd.DataFrame(
        {
            "unique_id": ["b1", "t1", "b2", "t2"],
            "cluster_id": [1, 1, 2, 3],
            "source_name": ["ballotready", "techspeed", "ballotready", "techspeed"],
        }
    )
    return input_df, pairwise_df, clustered_df


def test_run_summary_writes_per_provider_match_rates(tmp_path):
    """One cross-source cluster of two gives each provider a 1/2 match rate."""
    input_df, pairwise_df, clustered_df = _summary_frames()

    run_summary(input_df, pairwise_df, clustered_df, tmp_path)

    out = pd.read_csv(tmp_path / "audit_summary.csv")
    assert sorted(out["provider"]) == ["ballotready", "techspeed"]
    assert out["input_records"].tolist() == [2, 2]
    assert out["matched_records"].tolist() == [1, 1]
    assert out["match_rate"].tolist() == [0.5, 0.5]


def test_run_summary_handles_no_matches(tmp_path):
    """All-singleton clusters give a zero match rate rather than dividing by zero."""
    input_df, pairwise_df, clustered_df = _summary_frames()
    clustered_df["cluster_id"] = [1, 2, 3, 4]

    run_summary(input_df, pairwise_df.iloc[0:0], clustered_df, tmp_path)

    out = pd.read_csv(tmp_path / "audit_summary.csv")
    assert out["matched_records"].tolist() == [0, 0]
    assert out["match_rate"].tolist() == [0.0, 0.0]


# --- audit_low_confidence -----------------------------------------------------


def _pairwise_with_gammas(probs):
    n = len(probs)
    return pd.DataFrame(
        {
            "unique_id_l": [f"l{i}" for i in range(n)],
            "unique_id_r": [f"r{i}" for i in range(n)],
            "match_probability": probs,
            "match_weight": [0.0] * n,
            "source_name_l": ["ballotready"] * n,
            "source_name_r": ["techspeed"] * n,
            "gamma_last_name": [2] * n,
            "gamma_first_name": [1] * n,
        }
    )


def test_run_low_confidence_ranks_by_distance_from_half(tmp_path):
    """The most ambiguous pairs are the ones nearest 0.5, ordered by probability."""
    df = _pairwise_with_gammas([0.99, 0.52, 0.01, 0.48])

    out = run_low_confidence(df, tmp_path, CANDIDACY_CONFIG, sample_n=2)

    assert out["match_probability"].tolist() == [0.48, 0.52]
    assert (tmp_path / "audit_low_confidence.csv").exists()


def test_run_low_confidence_empty_input_returns_empty(tmp_path):
    """No predictions is a valid state, not an error."""
    out = run_low_confidence(_pairwise_with_gammas([]).iloc[0:0], tmp_path, CANDIDACY_CONFIG)

    assert out.empty
    assert not (tmp_path / "audit_low_confidence.csv").exists()


def test_run_low_confidence_keeps_gamma_nulls_as_null(tmp_path):
    """A missing gamma stays null instead of raising in the int() coercion."""
    df = _pairwise_with_gammas([0.5, 0.51])
    df.loc[0, "gamma_last_name"] = None

    out = run_low_confidence(df, tmp_path, CANDIDACY_CONFIG, sample_n=2)

    assert out["gamma_last_name"].isna().sum() == 1
    written = pd.read_csv(tmp_path / "audit_low_confidence.csv")
    assert written["gamma_last_name"].isna().sum() == 1


# --- audit_false_negatives: run_false_negatives -------------------------------


def _false_negative_frames(last_name_r="doe", **overrides):
    """Two providers with a plausible same-person pair left unmatched.

    Grouped on ["source_name", "state", "election_date"], so both records must
    share state and election_date to be considered candidates for each other.
    """
    base_l = {
        "unique_id": "b1",
        "source_name": "ballotready",
        "state": "WI",
        "election_date": "2026-11-03",
        "first_name": "jane",
        "last_name": "doe",
        "party": None,
        "email": None,
        "phone": None,
    }
    base_r = {**base_l, "unique_id": "t1", "source_name": "techspeed", "last_name": last_name_r}
    base_l.update(overrides)
    input_df = pd.DataFrame([base_l, base_r])
    # Both landed in their own cluster, i.e. the matcher did not pair them
    clustered_df = pd.DataFrame(
        {
            "unique_id": ["b1", "t1"],
            "cluster_id": [1, 2],
            "source_name": ["ballotready", "techspeed"],
            "last_name": [base_l["last_name"], last_name_r],
            "first_name": ["jane", "jane"],
            "state": ["WI", "WI"],
            "election_date": ["2026-11-03", "2026-11-03"],
            "email": [None, None],
            "phone": [None, None],
        }
    )
    pairwise_df = pd.DataFrame(columns=["unique_id_l", "unique_id_r", "match_probability"])
    return input_df, pairwise_df, clustered_df


def test_run_false_negatives_flags_similar_unmatched_pair(tmp_path):
    """Two singletons with near-identical names in the same group are suspicious."""
    input_df, pairwise_df, clustered_df = _false_negative_frames()

    out = run_false_negatives(input_df, pairwise_df, clustered_df, tmp_path, CANDIDACY_CONFIG)

    assert len(out) >= 1
    assert set(out["pair_status"]) == {"never_generated"}
    assert (tmp_path / "audit_false_negatives.csv").exists()


def test_run_false_negatives_ignores_dissimilar_names(tmp_path):
    """A different surname is not a false negative, so nothing is written."""
    input_df, pairwise_df, clustered_df = _false_negative_frames(last_name_r="kowalczyk")

    out = run_false_negatives(input_df, pairwise_df, clustered_df, tmp_path, CANDIDACY_CONFIG)

    assert out.empty
    assert not (tmp_path / "audit_false_negatives.csv").exists()


def test_run_false_negatives_skips_rows_with_null_group_key(tmp_path):
    """A null in a grouping column cannot build a lookup key, so the row is skipped.

    Nulled on both rows deliberately: the search runs from each singleton, so
    one intact row would still find the other from the opposite direction.
    """
    input_df, pairwise_df, clustered_df = _false_negative_frames()
    clustered_df["election_date"] = None

    out = run_false_negatives(input_df, pairwise_df, clustered_df, tmp_path, CANDIDACY_CONFIG)

    assert out.empty


def test_run_false_negatives_skips_rows_with_null_last_name(tmp_path):
    """A null surname gives nothing to compare, so the row is skipped."""
    input_df, pairwise_df, clustered_df = _false_negative_frames()
    clustered_df.loc[0, "last_name"] = None
    clustered_df.loc[1, "last_name"] = None

    out = run_false_negatives(input_df, pairwise_df, clustered_df, tmp_path, CANDIDACY_CONFIG)

    assert out.empty


def test_run_false_negatives_no_singletons_returns_empty(tmp_path):
    """When everything matched there are no singletons to inspect."""
    input_df, pairwise_df, clustered_df = _false_negative_frames()
    clustered_df["cluster_id"] = [1, 1]

    out = run_false_negatives(input_df, pairwise_df, clustered_df, tmp_path, CANDIDACY_CONFIG)

    assert out.empty
