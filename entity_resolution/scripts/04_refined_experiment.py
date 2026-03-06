"""
Refined experiment: remove br_race_id_int as a comparison (it's a race attribute,
not a candidate attribute), keep it only as a blocking rule.

Then measure how many false positives remain and what causes them.

Usage:
    cd prototyping/entity_resolution
    uv run python scripts/04_refined_experiment.py
"""

import json
import sys
from pathlib import Path

import pandas as pd
import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import Linker, SettingsCreator, block_on
from splink.comparison_library import CustomComparison
from splink.internals.duckdb.database_api import DuckDBAPI

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
RESULTS_DIR = PROJECT_DIR / "results"
INPUT_CSV = DATA_DIR / "input.csv"

PREDICT_THRESHOLD = 0.5
CLUSTER_THRESHOLD = 0.95


def load_and_prepare() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not INPUT_CSV.exists():
        print(f"ERROR: Input file not found: {INPUT_CSV}")
        sys.exit(1)

    df = pd.read_csv(INPUT_CSV, dtype=str)

    if "election_date" in df.columns:
        df["election_date"] = pd.to_datetime(
            df["election_date"], errors="coerce"
        ).dt.date.astype(str)
        df["election_date"] = df["election_date"].replace(
            {"NaT": None, "nan": None, "None": None}
        )

    df["first_name"] = df["first_name"].str.lower().str.strip()
    df["last_name"] = df["last_name"].str.lower().str.strip()

    if "first_name_aliases" in df.columns:
        df["first_name_aliases"] = df["first_name_aliases"].apply(
            lambda v: (
                json.loads(v)
                if isinstance(v, str)
                else v if isinstance(v, list) else [v]
            )
        )
    else:
        df["first_name_aliases"] = df["first_name"].apply(lambda n: [n])

    if "official_office_name" in df.columns:
        df["official_office_name"] = df["official_office_name"].str.lower().str.strip()

    if "district_identifier" in df.columns:
        df["district_identifier"] = (
            df["district_identifier"].str.lstrip("0").replace({"": "0"})
        )

    df["br_race_id_int"] = df["br_race_id"].where(
        df["br_race_id"].str.isnumeric().fillna(False), other=None
    )

    df = df.where(df.notna(), None)
    df = df.replace({"": None, "nan": None, "null": None})

    br_df = df[df["source_name"] == "ballotready"].copy()
    ts_df = df[df["source_name"] == "techspeed"].copy()
    print(f"Loaded: {len(br_df):,} BallotReady, {len(ts_df):,} TechSpeed")
    return br_df, ts_df


def build_first_name_comparison() -> CustomComparison:
    return CustomComparison(
        output_column_name="first_name",
        comparison_levels=[
            cll.NullLevel("first_name"),
            cll.ExactMatchLevel("first_name").configure(
                tf_adjustment_column="first_name",
            ),
            cll.ArrayIntersectLevel("first_name_aliases", min_intersection=1),
            cll.JaroWinklerLevel("first_name", distance_threshold=0.92),
            cll.ElseLevel(),
        ],
    )


def build_settings() -> SettingsCreator:
    """
    Key change: br_race_id_int is used ONLY as a blocking rule, NOT as a comparison.
    It identifies a race (multiple candidates per race), so using it as a comparison
    creates huge false-positive Bayes factors for same-race, different-candidate pairs.
    """
    return SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        comparisons=[
            cl.JaroWinklerAtThresholds(
                "last_name", score_threshold_or_thresholds=[0.95, 0.88]
            ).configure(term_frequency_adjustments=True),
            build_first_name_comparison(),
            cl.ExactMatch("state"),
            cl.ExactMatch("party"),
            cl.JaroWinklerAtThresholds(
                "official_office_name",
                score_threshold_or_thresholds=[0.96, 0.88],
            ),
            cl.ExactMatch("election_date"),
            cl.ExactMatch("election_stage"),
            cl.JaroWinklerAtThresholds("city", score_threshold_or_thresholds=[0.92]),
            cl.ExactMatch("district_identifier"),
            # br_race_id_int REMOVED as comparison — kept only as blocking rule
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
        ],
        blocking_rules_to_generate_predictions=[
            # Still use br_race_id_int for blocking — it efficiently generates
            # candidate pairs within the same race
            block_on("br_race_id_int"),
            block_on("state", "last_name"),
            block_on("official_office_name", "last_name"),
            block_on("state", "first_name", "official_office_name"),
            block_on("phone"),
            block_on("email"),
        ],
        retain_intermediate_calculation_columns=True,
        additional_columns_to_retain=[
            "source_name",
            "source_id",
            "candidate_office",
            "office_level",
            "office_type",
            "district_raw",
            "seat_name",
            "br_race_id",
            "br_candidacy_id",
        ],
    )


def train(linker: Linker) -> None:
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)
    for cols in [
        ("state", "election_date"),
        ("state", "last_name"),
        ("official_office_name",),
    ]:
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on(*cols),
            fix_u_probabilities=True,
        )


def classify_pair(row) -> str:
    """Classify what kind of false positive this is."""
    name_match = row["gamma_last_name"] > 0 and row["gamma_first_name"] > 0
    contact_match = row.get("gamma_email", -1) > 0 or row.get("gamma_phone", -1) > 0
    person_match = name_match or contact_match

    date_match = row["gamma_election_date"] > 0
    office_match = row["gamma_official_office_name"] > 0
    district_ok = (
        row["gamma_district_identifier"] != 0
    )  # -1 (null) is ok, 0 is mismatch

    if not person_match:
        return "different_person_same_race"
    elif person_match and not date_match:
        return "same_person_different_election_date"
    elif person_match and not office_match:
        return "same_person_different_office"
    elif person_match and not district_ok:
        return "same_person_different_district"
    else:
        return "true_positive"


def main():
    br_df, ts_df = load_and_prepare()
    settings = build_settings()
    linker = Linker([br_df, ts_df], settings, DuckDBAPI())
    train(linker)

    predictions = linker.inference.predict(
        threshold_match_probability=PREDICT_THRESHOLD
    )
    pairwise_df = predictions.as_pandas_dataframe()
    print(f"\nTotal pairs above {PREDICT_THRESHOLD}: {len(pairwise_df):,}")

    # Classify each pair
    pairwise_df["pair_type"] = pairwise_df.apply(classify_pair, axis=1)

    print("\nPair classification:")
    for ptype, count in pairwise_df["pair_type"].value_counts().items():
        prob_mean = pairwise_df[pairwise_df["pair_type"] == ptype][
            "match_probability"
        ].mean()
        print(f"  {ptype}: {count:,} (mean prob: {prob_mean:.3f})")

    # What does the probability distribution look like for each type?
    print("\nProbability distribution by pair type:")
    bins = [0.5, 0.7, 0.9, 0.95, 1.0]
    for ptype in [
        "true_positive",
        "different_person_same_race",
        "same_person_different_election_date",
        "same_person_different_office",
        "same_person_different_district",
    ]:
        subset = pairwise_df[pairwise_df["pair_type"] == ptype]
        if len(subset) == 0:
            continue
        print(f"\n  {ptype} ({len(subset):,} pairs):")
        for interval, count in (
            pd.cut(subset["match_probability"], bins=bins)
            .value_counts()
            .sort_index()
            .items()
        ):
            if count > 0:
                print(f"    {interval}: {count:,}")

    # Can we find a threshold that separates true positives from false positives?
    tp = pairwise_df[pairwise_df["pair_type"] == "true_positive"]
    fp_types = [
        "different_person_same_race",
        "same_person_different_election_date",
        "same_person_different_office",
        "same_person_different_district",
    ]
    fp = pairwise_df[pairwise_df["pair_type"].isin(fp_types)]

    if len(tp) > 0 and len(fp) > 0:
        print("\nThreshold analysis:")
        for thresh in [0.5, 0.7, 0.8, 0.9, 0.95, 0.99]:
            tp_above = (tp["match_probability"] >= thresh).sum()
            fp_above = (fp["match_probability"] >= thresh).sum()
            total_above = tp_above + fp_above
            precision = tp_above / total_above if total_above > 0 else 0
            recall = tp_above / len(tp) if len(tp) > 0 else 0
            print(
                f"  thresh={thresh}: TP={tp_above:,}, FP={fp_above:,}, precision={precision:.3f}, recall={recall:.3f}"
            )

    # Cluster and compare with vs without grain filter
    print(f"\n--- Clustering at threshold={CLUSTER_THRESHOLD} ---")

    # Without any filter
    clusters_raw = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions,
        threshold_match_probability=CLUSTER_THRESHOLD,
    )
    cdf_raw = clusters_raw.as_pandas_dataframe()
    n_clusters_raw = cdf_raw["cluster_id"].nunique()
    n_cross_raw = (cdf_raw.groupby("cluster_id")["source_dataset"].nunique() > 1).sum()

    # With grain filter (election_date + district only — NOT name or race ID)
    pred_table = predictions.physical_name
    db = linker._db_api._con
    db.execute(
        f"""
        CREATE OR REPLACE TABLE {pred_table} AS
        SELECT * FROM {pred_table}
        WHERE gamma_election_date > 0
          AND gamma_official_office_name > 0
          AND gamma_district_identifier != 0
          AND gamma_last_name > 0
          AND (gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)
    """
    )

    clusters_filtered = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions,
        threshold_match_probability=CLUSTER_THRESHOLD,
    )
    cdf_filtered = clusters_filtered.as_pandas_dataframe()
    n_clusters_filt = cdf_filtered["cluster_id"].nunique()
    n_cross_filt = (
        cdf_filtered.groupby("cluster_id")["source_dataset"].nunique() > 1
    ).sum()

    print(
        f"\n  Without filter: {n_clusters_raw:,} clusters, {n_cross_raw:,} cross-source"
    )
    print(
        f"  With grain filter: {n_clusters_filt:,} clusters, {n_cross_filt:,} cross-source"
    )

    # Save results
    pairwise_df.to_csv(RESULTS_DIR / "experiment_refined_pairwise.csv", index=False)
    cdf_filtered.to_csv(RESULTS_DIR / "experiment_refined_clusters.csv", index=False)
    print(f"\nResults saved to {RESULTS_DIR}/")


if __name__ == "__main__":
    main()
