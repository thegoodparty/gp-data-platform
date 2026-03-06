"""
Investigate records caught by the identity filter.

Runs the same Splink model as 01_initial_match.py but WITHOUT the post-prediction
identity filter, then analyzes the pairs that would have been removed.

Usage:
    cd prototyping/entity_resolution
    uv run python scripts/02_investigate_filter.py
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


def load_and_prepare() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Same prep as 01_initial_match.py."""
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
            cl.ExactMatch("br_race_id_int"),
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
        ],
        blocking_rules_to_generate_predictions=[
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


def main():
    br_df, ts_df = load_and_prepare()
    settings = build_settings()
    linker = Linker([br_df, ts_df], settings, DuckDBAPI())

    # Train
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

    # Predict WITHOUT the identity filter
    predictions = linker.inference.predict(
        threshold_match_probability=PREDICT_THRESHOLD
    )
    pairwise_df = predictions.as_pandas_dataframe()
    print(
        f"\nTotal pairwise predictions above {PREDICT_THRESHOLD}: {len(pairwise_df):,}"
    )

    # Identify which pairs the identity filter WOULD remove
    identity_ok = (
        (pairwise_df["gamma_election_date"] > 0)
        & (pairwise_df["gamma_official_office_name"] > 0)
        & (pairwise_df["gamma_district_identifier"] != 0)
        & (pairwise_df["gamma_br_race_id_int"] != 0)
        & (pairwise_df["gamma_last_name"] > 0)
        & (
            (pairwise_df["gamma_first_name"] > 0)
            | (pairwise_df["gamma_email"] > 0)
            | (pairwise_df["gamma_phone"] > 0)
        )
    )

    kept = pairwise_df[identity_ok]
    removed = pairwise_df[~identity_ok]

    print(f"\nKept by filter: {len(kept):,}")
    print(f"Removed by filter: {len(removed):,}")

    if len(removed) == 0:
        print("No records removed by filter - nothing to investigate!")
        return

    # Analyze WHY each pair was removed
    print("\n" + "=" * 70)
    print("ANALYSIS OF REMOVED PAIRS")
    print("=" * 70)

    # Which filter conditions triggered?
    reasons = {
        "election_date mismatch (gamma=0)": removed["gamma_election_date"] == 0,
        "election_date null (gamma=-1)": removed["gamma_election_date"] == -1,
        "office_name mismatch (gamma=0)": removed["gamma_official_office_name"] == 0,
        "office_name null (gamma=-1)": removed["gamma_official_office_name"] == -1,
        "district disagree (gamma=0)": removed["gamma_district_identifier"] == 0,
        "br_race_id disagree (gamma=0)": removed["gamma_br_race_id_int"] == 0,
        "last_name mismatch (gamma=0)": removed["gamma_last_name"] == 0,
        "last_name null (gamma=-1)": removed["gamma_last_name"] == -1,
        "first_name mismatch AND no email/phone": (
            (removed["gamma_first_name"] <= 0)
            & (removed["gamma_email"] <= 0)
            & (removed["gamma_phone"] <= 0)
        ),
    }

    print("\nFilter trigger counts (pairs can trigger multiple):")
    for reason, mask in reasons.items():
        count = mask.sum()
        if count > 0:
            print(f"  {reason}: {count:,}")

    # Match probability distribution of removed pairs
    print("\nMatch probability distribution of REMOVED pairs:")
    bins = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0]
    for interval, count in (
        pd.cut(removed["match_probability"], bins=bins)
        .value_counts()
        .sort_index()
        .items()
    ):
        if count > 0:
            print(f"  {interval}: {count:,}")

    # Show the Bayes factors for removed pairs to understand what's driving
    # high match probabilities despite identity mismatches
    bf_cols = [c for c in removed.columns if c.startswith("bf_")]
    gamma_cols = [c for c in removed.columns if c.startswith("gamma_")]

    print("\n\nMean Bayes factors for REMOVED vs KEPT pairs:")
    print(f"  {'Column':<30s} {'Removed':>10s} {'Kept':>10s}")
    print(f"  {'-'*30} {'-'*10} {'-'*10}")
    for col in bf_cols:
        r_mean = removed[col].mean()
        k_mean = kept[col].mean() if len(kept) > 0 else float("nan")
        print(f"  {col:<30s} {r_mean:>10.3f} {k_mean:>10.3f}")

    print("\n\nGamma distribution for REMOVED pairs:")
    for col in gamma_cols:
        dist = removed[col].value_counts().sort_index()
        print(f"\n  {col}:")
        for val, count in dist.items():
            print(f"    gamma={val}: {count:,}")

    # Show detailed examples of high-probability removed pairs
    high_prob_removed = removed[removed["match_probability"] >= 0.9].sort_values(
        "match_probability", ascending=False
    )
    print(f"\n\nHigh-probability removed pairs (>=0.9): {len(high_prob_removed):,}")

    display_cols = [
        "match_probability",
        "match_weight",
        "first_name_l",
        "first_name_r",
        "gamma_first_name",
        "last_name_l",
        "last_name_r",
        "gamma_last_name",
        "state_l",
        "state_r",
        "gamma_state",
        "official_office_name_l",
        "official_office_name_r",
        "gamma_official_office_name",
        "election_date_l",
        "election_date_r",
        "gamma_election_date",
        "district_identifier_l",
        "district_identifier_r",
        "gamma_district_identifier",
        "br_race_id_int_l",
        "br_race_id_int_r",
        "gamma_br_race_id_int",
        "email_l",
        "email_r",
        "gamma_email",
        "phone_l",
        "phone_r",
        "gamma_phone",
        "city_l",
        "city_r",
        "gamma_city",
        "party_l",
        "party_r",
        "gamma_party",
        "election_stage_l",
        "election_stage_r",
        "gamma_election_stage",
    ]
    existing_cols = [c for c in display_cols if c in removed.columns]

    for i, (_, row) in enumerate(high_prob_removed.head(20).iterrows()):
        print(
            f"\n--- Removed pair #{i+1} (prob={row['match_probability']:.4f}, weight={row['match_weight']:.2f}) ---"
        )
        for col in existing_cols:
            val = row[col]
            print(f"  {col:<40s} {val}")

    # Save removed pairs for manual review
    removed.to_csv(RESULTS_DIR / "removed_by_filter.csv", index=False)
    print(f"\nFull removed pairs saved to {RESULTS_DIR / 'removed_by_filter.csv'}")


if __name__ == "__main__":
    main()
