"""
Splink entity resolution: BallotReady x TechSpeed candidacies.

Usage:
    cd entity_resolution
    uv run er-match --input data/input.csv
    uv run er-match --input data/input.csv --output-dir results/

Input:  CSV file with prematch candidacy records
Output: results/pairwise_predictions.csv
        results/clustered_candidacies.csv
"""

import json
from pathlib import Path

import pandas as pd
import splink.comparison_level_library as cll
import splink.internals.comparison_library as cl
from splink import Linker, SettingsCreator, block_on
from splink.blocking_rule_library import CustomRule
from splink.comparison_library import CustomComparison
from splink.internals.duckdb.database_api import DuckDBAPI

PREDICT_THRESHOLD = 0.5
CLUSTER_THRESHOLD = 0.95


def load_and_prepare(input_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load prematch CSV, clean nulls, return (br_df, ts_df)."""
    df = pd.read_csv(input_path, dtype=str)
    print(f"Loaded {len(df):,} rows from {input_path}")
    print(f"\nSource distribution:\n{df['source_name'].value_counts().to_string()}")

    if "election_date" in df.columns:
        df["election_date"] = pd.to_datetime(
            df["election_date"], errors="coerce"
        ).dt.date.astype(str)
        df["election_date"] = df["election_date"].replace(
            {"NaT": None, "nan": None, "None": None}
        )

    df["first_name"] = df["first_name"].str.lower().str.strip()
    df["last_name"] = df["last_name"].str.lower().str.strip()

    # first_name_aliases is built upstream in the dbt prematch model
    # (int__er_prematch_candidacy_stages) via the nicknames seed and arrives as a
    # JSON array string (always includes the first_name itself).
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

    # Strip leading zeros from district_identifier ("01" → "1") so
    # BallotReady and TechSpeed formatting differences don't cause mismatches.
    if "district_identifier" in df.columns:
        df["district_identifier"] = (
            df["district_identifier"]
            .str.lstrip("0")
            .replace({"": "0"})  # preserve bare "0"
        )

    # br_race_id_int: keep only integer race IDs (BR-originated).
    # Non-integer values like "ts_found_race_net_new" become null so the
    # blocking rule only fires for records with a shared BR race ID.
    # NOTE: br_race_id_int is used ONLY for blocking, not as a comparison.
    # It identifies a race (multiple candidates per race), so using it as a
    # comparison creates huge false-positive Bayes factors for same-race,
    # different-candidate pairs.
    df["br_race_id_int"] = df["br_race_id"].where(
        df["br_race_id"].str.isnumeric().fillna(False), other=None
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


def build_first_name_comparison() -> CustomComparison:
    """Custom first_name comparison: exact → nickname → fuzzy → else."""
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
    # Comparisons use only candidate-level attributes — fields that distinguish
    # one candidate from another within the same race.
    #
    # Race/election-level attributes (state, office, date, district, city,
    # election_stage, br_race_id) are used only in blocking rules to generate
    # candidate pairs. They don't carry signal for distinguishing candidates
    # within a race, and including them as comparisons inflates match scores
    # for same-race, different-candidate pairs.
    return SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        comparisons=[
            # --- Candidate-level comparisons ---
            cl.JaroWinklerAtThresholds(
                "last_name", score_threshold_or_thresholds=[0.95, 0.88]
            ).configure(term_frequency_adjustments=True),
            build_first_name_comparison(),
            cl.ExactMatch("party"),
            cl.ExactMatch("email"),
            cl.ExactMatch("phone"),
        ],
        blocking_rules_to_generate_predictions=[
            # Race/election-level blocking rules generate candidate pairs that
            # plausibly refer to the same candidacy.
            #
            # br_race_id_int: high-cardinality race ID — covers most TS
            # records that originated from BR.
            block_on("br_race_id_int"),
            # Fuzzy office name blocking catches cross-source format
            # differences like "republic city council - ward 3" vs
            # "republic city ward 3" (JW 0.88).
            CustomRule(
                "l.state = r.state"
                " AND l.election_date = r.election_date"
                " AND jaro_winkler_similarity(l.official_office_name,"
                " r.official_office_name) >= 0.88"
                " AND l.last_name = r.last_name",
                sql_dialect="duckdb",
            ),
            block_on("state", "last_name", "election_date"),
            # Fuzzy last name + fuzzy office blocking catches typos like
            # "feidler" vs "fiedler" or "montelone" vs "monteleone" with
            # cross-source office formatting differences.
            CustomRule(
                "l.state = r.state"
                " AND l.election_date = r.election_date"
                " AND jaro_winkler_similarity(l.official_office_name,"
                " r.official_office_name) >= 0.88"
                " AND jaro_winkler_similarity(l.last_name,"
                " r.last_name) >= 0.88",
                sql_dialect="duckdb",
            ),
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
            "district_identifier",
            "seat_name",
            "br_race_id",
            "br_candidacy_id",
            "official_office_name",
            "election_date",
            "election_stage",
            "state",
            "city",
        ],
    )


def train_model(linker: Linker) -> None:
    print("\n--- Training ---")
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)
    # EM training blocks on comparison columns to estimate m probabilities.
    # Each pass fixes the blocked columns and estimates the rest.
    for cols in [
        ("last_name",),
        ("first_name",),
        ("email",),
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

    # Person identity filter: the br_race_id_int blocking rule generates all
    # candidate pairs within a race, including different candidates. Since
    # race-level attributes are no longer comparisons, the model can only
    # distinguish candidates by name/email/phone. Require at minimum that
    # the last name agrees and the first name or contact info agrees.
    pre = len(pairwise_df)
    person_ok = (pairwise_df["gamma_last_name"] > 0) & (
        (pairwise_df["gamma_first_name"] > 0)
        | (pairwise_df["gamma_email"] > 0)
        | (pairwise_df["gamma_phone"] > 0)
    )
    pairwise_df = pairwise_df[person_ok].copy()
    dropped = pre - len(pairwise_df)
    if dropped > 0:
        print(f"Person identity filter: removed {dropped:,} pairs")

    # Apply the same filter in DuckDB for clustering
    pred_table = predictions.physical_name
    db = linker._db_api._con
    db.execute(
        f"""
        CREATE OR REPLACE TABLE {pred_table} AS
        SELECT * FROM {pred_table}
        WHERE gamma_last_name > 0
          AND (gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)
    """
    )

    print("\nMatch probability distribution:")
    bins = [0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0]
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
    linker: Linker,
    pairwise_df: pd.DataFrame,
    clustered_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    pairwise_df.to_csv(output_dir / "pairwise_predictions.csv", index=False)
    if len(clustered_df) > 0:
        clustered_df.to_csv(output_dir / "clustered_candidacies.csv", index=False)

    for name, method in [
        ("match_weights", "match_weights_chart"),
        ("m_u_parameters", "m_u_parameters_chart"),
    ]:
        try:
            chart = getattr(linker.visualisations, method)()
            chart.save(str(output_dir / f"{name}_chart.html"))
            chart.save(str(output_dir / f"{name}_chart.png"), scale_factor=2)
        except Exception as e:
            print(f"Could not save {name} chart: {e}")

    print(f"\nResults saved to {output_dir}/")


def run(input_path: Path, output_dir: Path) -> None:
    print("=" * 60)
    print("Splink Entity Resolution: BallotReady x TechSpeed Candidacies")
    print("=" * 60)

    br_df, ts_df = load_and_prepare(input_path)
    settings = build_settings()
    linker = Linker([br_df, ts_df], settings, DuckDBAPI())

    train_model(linker)
    pairwise_df, clustered_df = predict_and_cluster(linker)
    save_results(linker, pairwise_df, clustered_df, output_dir)

    print("\nDone!")


if __name__ == "__main__":
    # Default paths for direct script invocation
    script_dir = Path(__file__).resolve().parent
    project_dir = script_dir.parent
    run(
        input_path=project_dir / "data" / "input.csv",
        output_dir=project_dir / "results",
    )
