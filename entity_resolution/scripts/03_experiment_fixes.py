"""
Experiment with Splink settings to eliminate the need for the identity filter.

The core problem: same-race, different-candidate pairs share office, state,
election_date, district, and br_race_id — generating massive positive Bayes
factors that overwhelm the name-mismatch penalty.

Experiments:
1. Baseline (current model, no filter) — quantify false positives
2. Remove br_race_id_int as a comparison (it's too dominant for same-race pairs)
3. Remove br_race_id_int AND reduce official_office_name to ExactMatch
4. Add explicit "different name" penalty levels (negative Bayes factor for gamma=0)

Usage:
    cd prototyping/entity_resolution
    uv run python scripts/03_experiment_fixes.py
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


def make_blocking_rules(include_race_id: bool = True):
    rules = []
    if include_race_id:
        rules.append(block_on("br_race_id_int"))
    rules.extend(
        [
            block_on("state", "last_name"),
            block_on("official_office_name", "last_name"),
            block_on("state", "first_name", "official_office_name"),
            block_on("phone"),
            block_on("email"),
        ]
    )
    return rules


def make_comparisons(include_race_id: bool = True):
    comps = [
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
    ]
    if include_race_id:
        comps.append(cl.ExactMatch("br_race_id_int"))
    comps.extend(
        [
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
        ]
    )
    return comps


def build_settings(include_race_id: bool = True) -> SettingsCreator:
    return SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        comparisons=make_comparisons(include_race_id),
        blocking_rules_to_generate_predictions=make_blocking_rules(include_race_id),
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


def evaluate(pairwise_df: pd.DataFrame, label: str) -> dict:
    """Evaluate predictions against the identity filter criteria as ground truth."""
    # The identity filter represents our ground truth for what should NOT match
    identity_ok = (
        (pairwise_df["gamma_election_date"] > 0)
        & (pairwise_df["gamma_official_office_name"] > 0)
        & (
            pairwise_df.get("gamma_district_identifier", pd.Series(dtype=float)).fillna(
                1
            )
            != 0
        )
        & (
            pairwise_df.get("gamma_br_race_id_int", pd.Series(dtype=float)).fillna(1)
            != 0
        )
        & (pairwise_df["gamma_last_name"] > 0)
        & (
            (pairwise_df["gamma_first_name"] > 0)
            | (pairwise_df.get("gamma_email", pd.Series(dtype=float)).fillna(0) > 0)
            | (pairwise_df.get("gamma_phone", pd.Series(dtype=float)).fillna(0) > 0)
        )
    )

    total = len(pairwise_df)
    true_pos = identity_ok.sum()
    false_pos = total - true_pos

    # Also check: how many false positives are above various thresholds?
    high_fp = {}
    for thresh in [0.8, 0.9, 0.95, 0.99]:
        above = pairwise_df[pairwise_df["match_probability"] >= thresh]
        fp_above = (~identity_ok[above.index]).sum()
        high_fp[thresh] = fp_above

    result = {
        "total_pairs": total,
        "true_positives": int(true_pos),
        "false_positives": int(false_pos),
        "fp_rate": false_pos / total if total > 0 else 0,
        "high_prob_fps": high_fp,
    }

    print(f"\n  [{label}]")
    print(f"  Total pairs above {PREDICT_THRESHOLD}: {total:,}")
    print(f"  True positives (would pass filter): {true_pos:,}")
    print(f"  False positives (would be filtered): {false_pos:,}")
    print(f"  FP rate: {result['fp_rate']:.1%}")
    for thresh, fp in high_fp.items():
        above_count = len(pairwise_df[pairwise_df["match_probability"] >= thresh])
        print(f"  FPs at >= {thresh}: {fp:,} / {above_count:,}")

    return result


def run_experiment(br_df, ts_df, label: str, include_race_id: bool = True):
    print(f"\n{'='*60}")
    print(f"EXPERIMENT: {label}")
    print(f"{'='*60}")

    settings = build_settings(include_race_id)
    linker = Linker([br_df.copy(), ts_df.copy()], settings, DuckDBAPI())
    train(linker)

    predictions = linker.inference.predict(
        threshold_match_probability=PREDICT_THRESHOLD
    )
    pairwise_df = predictions.as_pandas_dataframe()

    result = evaluate(pairwise_df, label)

    # Also cluster on filtered results and report
    gamma_cols_available = [c for c in pairwise_df.columns if c.startswith("gamma_")]

    filter_clauses = [
        "gamma_election_date > 0",
        "gamma_official_office_name > 0",
        "gamma_last_name > 0",
    ]
    if "gamma_district_identifier" in gamma_cols_available:
        filter_clauses.append("gamma_district_identifier != 0")
    if "gamma_br_race_id_int" in gamma_cols_available:
        filter_clauses.append("gamma_br_race_id_int != 0")
    filter_clauses.append(
        "(gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)"
    )

    # Count what clustering looks like WITHOUT filter
    clusters_no_filter = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions,
        threshold_match_probability=CLUSTER_THRESHOLD,
    )
    cdf_no_filter = clusters_no_filter.as_pandas_dataframe()
    n_clusters_nf = cdf_no_filter["cluster_id"].nunique()
    n_cross_nf = (
        cdf_no_filter.groupby("cluster_id")["source_dataset"].nunique() > 1
    ).sum()
    print("\n  Clustering WITHOUT identity filter:")
    print(f"    Clusters: {n_clusters_nf:,}, Cross-source: {n_cross_nf:,}")

    # Show largest clusters (potential many-to-many issues)
    cluster_sizes = (
        cdf_no_filter.groupby("cluster_id").size().sort_values(ascending=False)
    )
    large_clusters = cluster_sizes[cluster_sizes > 2]
    if len(large_clusters) > 0:
        print(f"    Clusters with >2 members: {len(large_clusters):,}")
        print(f"    Largest cluster size: {cluster_sizes.iloc[0]}")

    return result, pairwise_df


def main():
    br_df, ts_df = load_and_prepare()
    print(f"Data: {len(br_df):,} BallotReady, {len(ts_df):,} TechSpeed\n")

    results = {}

    # Experiment 1: Baseline (with br_race_id_int as comparison)
    results["baseline"], pw_baseline = run_experiment(
        br_df, ts_df, "Baseline (current model, no filter)", include_race_id=True
    )

    # Experiment 2: Remove br_race_id_int as comparison
    # br_race_id identifies a RACE, not a candidate. Multiple candidates run in
    # the same race, so matching on it creates massive false-positive signal.
    results["no_race_id"], pw_no_race = run_experiment(
        br_df, ts_df, "Remove br_race_id_int comparison", include_race_id=False
    )

    # Summary
    print(f"\n\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"{'Experiment':<45s} {'Total':>8s} {'FPs':>8s} {'FP%':>8s}")
    print(f"{'-'*45} {'-'*8} {'-'*8} {'-'*8}")
    for name, r in results.items():
        print(
            f"{name:<45s} {r['total_pairs']:>8,} {r['false_positives']:>8,} {r['fp_rate']:>7.1%}"
        )

    # Detailed look at false positives remaining in the no_race_id experiment
    if results["no_race_id"]["false_positives"] > 0:
        print("\n\nRemaining false positives in no_race_id experiment:")
        # Reconstruct identity filter
        identity_ok = (
            (pw_no_race["gamma_election_date"] > 0)
            & (pw_no_race["gamma_official_office_name"] > 0)
            & (pw_no_race["gamma_district_identifier"] != 0)
            & (pw_no_race["gamma_last_name"] > 0)
            & (
                (pw_no_race["gamma_first_name"] > 0)
                | (pw_no_race["gamma_email"] > 0)
                | (pw_no_race["gamma_phone"] > 0)
            )
        )
        remaining_fps = pw_no_race[~identity_ok]
        print(f"  Count: {len(remaining_fps):,}")
        print("\n  Probability distribution:")
        bins = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0]
        for interval, count in (
            pd.cut(remaining_fps["match_probability"], bins=bins)
            .value_counts()
            .sort_index()
            .items()
        ):
            if count > 0:
                print(f"    {interval}: {count:,}")

        # What's driving remaining FPs?
        print("\n  Trigger reasons:")
        triggers = {
            "election_date mismatch": remaining_fps["gamma_election_date"] == 0,
            "office_name mismatch": remaining_fps["gamma_official_office_name"] == 0,
            "district disagree": remaining_fps["gamma_district_identifier"] == 0,
            "last_name mismatch": remaining_fps["gamma_last_name"] == 0,
            "name mismatch, no email/phone": (
                (remaining_fps["gamma_first_name"] <= 0)
                & (remaining_fps["gamma_email"] <= 0)
                & (remaining_fps["gamma_phone"] <= 0)
            ),
        }
        for reason, mask in triggers.items():
            count = mask.sum()
            if count > 0:
                print(f"    {reason}: {count:,}")

        # Show some examples
        for i, (_, row) in enumerate(
            remaining_fps.sort_values("match_probability", ascending=False)
            .head(10)
            .iterrows()
        ):
            print(f"\n  --- FP #{i+1} (prob={row['match_probability']:.4f}) ---")
            for col_pair in [
                ("first_name", "gamma_first_name"),
                ("last_name", "gamma_last_name"),
                ("official_office_name", "gamma_official_office_name"),
                ("election_date", "gamma_election_date"),
                ("district_identifier", "gamma_district_identifier"),
                ("email", "gamma_email"),
                ("phone", "gamma_phone"),
            ]:
                field, gamma = col_pair
                l_col = f"{field}_l"
                r_col = f"{field}_r"
                if l_col in row.index:
                    print(
                        f"    {field}: '{row[l_col]}' vs '{row[r_col]}' (gamma={row[gamma]})"
                    )


if __name__ == "__main__":
    main()
