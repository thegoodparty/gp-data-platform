"""
Splink entity resolution: BallotReady x TechSpeed candidacies.

Usage:
    cd prototyping/entity_resolution
    uv run python scripts/01_initial_match.py

Input:  data/input.csv
Output: results/pairwise_predictions.csv
        results/clustered_candidacies.csv
"""

import sys
from pathlib import Path

import pandas as pd
import splink.internals.comparison_library as cl
from splink import Linker, SettingsCreator, block_on
from splink.internals.duckdb.database_api import DuckDBAPI

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
RESULTS_DIR = PROJECT_DIR / "results"
INPUT_CSV = DATA_DIR / "input.csv"

RESULTS_DIR.mkdir(parents=True, exist_ok=True)

PREDICT_THRESHOLD = 0.2
CLUSTER_THRESHOLD = 0.7


def load_and_prepare() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load prematch CSV, clean nulls, return (br_df, ts_df)."""
    if not INPUT_CSV.exists():
        print(f"ERROR: Input file not found: {INPUT_CSV}")
        sys.exit(1)

    df = pd.read_csv(INPUT_CSV, dtype=str)
    print(f"Loaded {len(df):,} rows from {INPUT_CSV.name}")
    print(f"\nSource distribution:\n{df['source_name'].value_counts().to_string()}")

    for col in ["general_election_date", "primary_election_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date.astype(str)
            df[col] = df[col].replace({"NaT": None, "nan": None, "None": None})

    df["first_name"] = df["first_name"].str.lower().str.strip()
    df["last_name"] = df["last_name"].str.lower().str.strip()

    # Normalize city: strip suffixes that differ between sources
    # (e.g. "Kenosha County" vs "Kenosha", "Saint Louis" vs "St. Louis")
    if "city" in df.columns:
        df["city"] = (
            df["city"]
            .str.strip()
            .str.replace(r"\s+County$", "", regex=True)
            .str.replace(r"\s+City$", "", regex=True)
            .str.replace(r"\bSaint\b", "St.", regex=True)
            .str.replace(r"\bSt\b", "St.", regex=True)
        )

    df = df.where(df.notna(), None)
    df = df.replace({"": None, "nan": None, "null": None})

    print("\nColumn population rates:")
    for col in df.columns:
        pop = df[col].notna().sum()
        print(f"  {col:30s} {pop:>8,} / {len(df):,}  ({pop / len(df) * 100:5.1f}%)")

    br_df = df[df["source_name"] == "ballotready"].copy()
    ts_df = df[df["source_name"] == "techspeed"].copy()
    print(f"\nSplit: {len(br_df):,} BallotReady, {len(ts_df):,} TechSpeed")

    return br_df, ts_df


def build_settings() -> SettingsCreator:
    return SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        comparisons=[
            cl.JaroWinklerAtThresholds(
                "last_name", score_threshold_or_thresholds=[0.95, 0.88]
            ),
            cl.JaroWinklerAtThresholds(
                "first_name", score_threshold_or_thresholds=[0.92, 0.85]
            ),
            cl.ExactMatch("state"),
            cl.ExactMatch("party"),
            # Relaxed JW thresholds — naming conventions differ across sources
            # (e.g. "Town Supervisor"↔"Township Supervisor" JW=0.905)
            cl.JaroWinklerAtThresholds(
                "candidate_office", score_threshold_or_thresholds=[0.88, 0.70]
            ),
            cl.JaroWinklerAtThresholds("city", score_threshold_or_thresholds=[0.92]),
            cl.ExactMatch("general_election_date"),
            cl.ExactMatch("primary_election_date"),
            cl.ExactMatch("district_identifier"),
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("state", "last_name"),
            block_on("state", "first_name", "candidate_office"),
        ],
        retain_intermediate_calculation_columns=True,
        additional_columns_to_retain=[
            "source_name",
            "source_id",
            "gp_candidacy_id",
            "official_office_name",
            "office_level",
            "office_type",
            "district_raw",
            "seat_name",
            "br_race_id",
        ],
    )


def train_model(linker: Linker) -> None:
    print("\n--- Training ---")
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)
    # Three EM passes with different blocking to cover all comparisons:
    #   Pass 1: blocks state + date → trains name, office, district, contact
    #   Pass 2: blocks state + last_name → trains first_name, date, office, district
    #   Pass 3: blocks last + first name → trains state
    for cols in [
        ("state", "general_election_date"),
        ("state", "last_name"),
        ("last_name", "first_name"),
    ]:
        print(f"  EM pass: blocking on {' + '.join(cols)}...")
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on(*cols),
            fix_u_probabilities=True,
        )
    print("Training complete.")


def predict_and_cluster(linker: Linker) -> tuple[pd.DataFrame, pd.DataFrame]:
    print("\n--- Prediction ---")
    predictions = linker.inference.predict(
        threshold_match_probability=PREDICT_THRESHOLD
    )
    pairwise_df = predictions.as_pandas_dataframe()
    print(f"Pairwise predictions: {len(pairwise_df):,} pairs above {PREDICT_THRESHOLD}")

    if len(pairwise_df) == 0:
        print("WARNING: No predictions found.")
        return pairwise_df, pd.DataFrame()

    print("\nMatch probability distribution:")
    bins = [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0]
    for interval, count in (
        pd.cut(pairwise_df["match_probability"], bins=bins)
        .value_counts()
        .sort_index()
        .items()
    ):
        print(f"  {interval}: {count:,}")

    print(f"\n--- Clustering (threshold={CLUSTER_THRESHOLD}) ---")
    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions,
        threshold_match_probability=CLUSTER_THRESHOLD,
    )
    clustered_df = clusters.as_pandas_dataframe()

    n_clusters = clustered_df["cluster_id"].nunique()
    multi = clustered_df.groupby("cluster_id").size()
    n_matched = (multi > 1).sum()
    n_cross = (clustered_df.groupby("cluster_id")["source_dataset"].nunique() > 1).sum()
    print(
        f"Total clusters: {n_clusters:,}  |  Matched (2+): {n_matched:,}  |  Cross-source: {n_cross:,}"
    )

    within = n_matched - n_cross
    if within > 0:
        print(f"WARNING: {within} within-source duplicate clusters found")

    return pairwise_df, clustered_df


def save_results(
    linker: Linker, pairwise_df: pd.DataFrame, clustered_df: pd.DataFrame
) -> None:
    pairwise_df.to_csv(RESULTS_DIR / "pairwise_predictions.csv", index=False)
    if len(clustered_df) > 0:
        clustered_df.to_csv(RESULTS_DIR / "clustered_candidacies.csv", index=False)

    for name, method in [
        ("match_weights", "match_weights_chart"),
        ("m_u_parameters", "m_u_parameters_chart"),
    ]:
        try:
            chart = getattr(linker.visualisations, method)()
            chart.save(str(RESULTS_DIR / f"{name}_chart.html"))
            chart.save(str(RESULTS_DIR / f"{name}_chart.png"), scale_factor=2)
        except Exception as e:
            print(f"Could not save {name} chart: {e}")

    print(f"\nResults saved to {RESULTS_DIR}/")


def main():
    print("=" * 60)
    print("Splink Entity Resolution: BallotReady x TechSpeed Candidacies")
    print("=" * 60)

    br_df, ts_df = load_and_prepare()
    settings = build_settings()
    linker = Linker([br_df, ts_df], settings, DuckDBAPI())

    train_model(linker)
    pairwise_df, clustered_df = predict_and_cluster(linker)
    save_results(linker, pairwise_df, clustered_df)

    print("\nDone!")


if __name__ == "__main__":
    main()
