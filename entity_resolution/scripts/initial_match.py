"""
Splink entity resolution: BallotReady x TechSpeed candidacies.

Usage:
    cd entity_resolution
    uv run python scripts/cli.py match --input data/input.csv
    uv run python scripts/cli.py match --input data/input.csv --output-dir results/

Input:  CSV exported from int__er_prematch_candidacy_stages
Output: results/pairwise_predictions.csv
        results/clustered_candidacies.csv
        results/match_weights_chart.{html,png}
        results/m_u_parameters_chart.{html,png}
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

PREDICT_THRESHOLD = 0.01
CLUSTER_THRESHOLD = 0.95


def load_and_prepare(input_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load prematch CSV, clean nulls, return (br_df, ts_df)."""
    df = pd.read_csv(input_path, dtype=str)
    print(f"Loaded {len(df):,} rows from {input_path}")
    print(f"\nSource distribution:\n{df['source_name'].value_counts().to_string()}")

    df["election_date"] = pd.to_datetime(
        df["election_date"], errors="coerce"
    ).dt.date.astype(str)
    df["election_date"] = df["election_date"].replace(
        {"NaT": None, "nan": None, "None": None}
    )
    df["first_name"] = df["first_name"].str.lower().str.strip()
    df["last_name"] = df["last_name"].str.lower().str.strip()
    df["official_office_name"] = df["official_office_name"].str.lower().str.strip()

    # Parse first_name_aliases from JSON array string built by the dbt prematch model
    df["first_name_aliases"] = df["first_name_aliases"].apply(
        lambda v: json.loads(v) if isinstance(v, str) else [v]
    )

    # Normalize district formatting ("01" → "1")
    df["district_identifier"] = (
        df["district_identifier"].str.lstrip("0").replace({"": "0"})
    )

    # Keep only integer race IDs for blocking (non-integer values like
    # "ts_found_race_net_new" become null)
    df["br_race_id_int"] = df["br_race_id"].where(
        df["br_race_id"].str.isnumeric().fillna(False), other=None
    )

    # Normalize nulls so Splink treats missing data correctly
    df = df.where(df.notna(), None)
    df = df.replace({"": None, "nan": None, "null": None})

    br_df = df[df["source_name"] == "ballotready"].copy()
    ts_df = df[df["source_name"] == "techspeed"].copy()
    print(f"Split: {len(br_df):,} BallotReady, {len(ts_df):,} TechSpeed")

    return br_df, ts_df


SETTINGS = SettingsCreator(
    link_type="link_only",
    unique_id_column_name="unique_id",
    comparisons=[
        # ── Candidate-level ──
        cl.JaroWinklerAtThresholds(
            "last_name", score_threshold_or_thresholds=[0.95, 0.88]
        ).configure(term_frequency_adjustments=True),
        CustomComparison(
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
        ),
        cl.ExactMatch("party"),
        cl.ExactMatch("email"),
        cl.ExactMatch("phone"),
        # ── Race / election-level ──
        # These provide evidence that two candidates ran in the same race.
        # The person identity filter (below) prevents same-race,
        # different-candidate pairs from being false positives.
        cl.ExactMatch("state"),
        cl.ExactMatch("election_date"),
        cl.JaroWinklerAtThresholds(
            "official_office_name", score_threshold_or_thresholds=[0.95, 0.88]
        ),
        cl.ExactMatch("city"),
    ],
    blocking_rules_to_generate_predictions=[
        block_on("br_race_id_int"),
        CustomRule(
            "l.state = r.state"
            " AND l.election_date = r.election_date"
            " AND jaro_winkler_similarity(l.official_office_name,"
            " r.official_office_name) >= 0.88"
            " AND l.last_name = r.last_name",
            sql_dialect="duckdb",
        ),
        block_on("state", "last_name", "election_date"),
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
        "election_stage",
    ],
)

EM_TRAINING_BLOCKS = [
    ("last_name",),
    ("first_name",),
    ("email",),
    ("state", "election_date"),
]


def train_model(linker: Linker) -> None:
    """Estimate u via random sampling, then m via EM on each comparison column."""
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)
    for cols in EM_TRAINING_BLOCKS:
        linker.training.estimate_parameters_using_expectation_maximisation(
            block_on(*cols), fix_u_probabilities=True
        )


def predict_and_cluster(linker: Linker) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Predict matches, apply person identity filter, cluster."""
    predictions = linker.inference.predict(
        threshold_match_probability=PREDICT_THRESHOLD
    )
    pairwise_df = predictions.as_pandas_dataframe()
    print(f"Pairwise predictions: {len(pairwise_df):,} pairs above {PREDICT_THRESHOLD}")

    if len(pairwise_df) == 0:
        print("WARNING: No predictions found.")
        return pairwise_df, pd.DataFrame()

    # Person identity filter: require last name agreement + first name or
    # contact info agreement to remove same-race different-candidate pairs.
    pre = len(pairwise_df)
    person_ok = (pairwise_df["gamma_last_name"] > 0) & (
        (pairwise_df["gamma_first_name"] > 0)
        | (pairwise_df["gamma_email"] > 0)
        | (pairwise_df["gamma_phone"] > 0)
    )
    # Race-level filter: require that at least the office name or city agrees.
    # Without this, common-name pairs in the same state/election but different
    # offices can score above the cluster threshold.
    race_ok = (pairwise_df["gamma_official_office_name"] > 0) | (
        pairwise_df["gamma_city"] > 0
    )
    # Exclude pairs where both sides have a known (integer) br_race_id and
    # they differ — these are confirmed different candidacy stages.
    both_race_known = (
        pairwise_df["br_race_id_int_l"].notna()
        & pairwise_df["br_race_id_int_r"].notna()
    )
    same_race_id = pairwise_df["br_race_id_int_l"] == pairwise_df["br_race_id_int_r"]
    race_id_ok = ~both_race_known | same_race_id
    pairwise_df = pairwise_df[person_ok & race_ok & race_id_ok].copy()
    if (dropped := pre - len(pairwise_df)) > 0:
        print(f"Person + race filter: removed {dropped:,} pairs")

    # Apply the same filter in DuckDB for clustering
    pred_table = predictions.physical_name
    linker._db_api._con.execute(
        f"""
        CREATE OR REPLACE TABLE {pred_table} AS
        SELECT * FROM {pred_table}
        WHERE gamma_last_name > 0
          AND (gamma_first_name > 0 OR gamma_email > 0 OR gamma_phone > 0)
          AND (gamma_official_office_name > 0 OR gamma_city > 0)
          AND NOT (
            br_race_id_int_l IS NOT NULL
            AND br_race_id_int_r IS NOT NULL
            AND br_race_id_int_l != br_race_id_int_r
          )
    """
    )

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions, threshold_match_probability=CLUSTER_THRESHOLD
    )
    clustered_df = clusters.as_pandas_dataframe()

    n_matched = (clustered_df.groupby("cluster_id").size() > 1).sum()
    n_cross = (clustered_df.groupby("cluster_id")["source_dataset"].nunique() > 1).sum()
    print(f"Matched clusters: {n_matched:,}  |  Cross-source: {n_cross:,}")
    if (within := n_matched - n_cross) > 0:
        print(f"WARNING: {within} within-source duplicate clusters found")

    return pairwise_df, clustered_df


def save_results(
    linker: Linker,
    pairwise_df: pd.DataFrame,
    clustered_df: pd.DataFrame,
    output_dir: Path,
) -> None:
    """Write CSVs and diagnostic charts."""
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
    """Load data, train, predict, cluster, save."""
    br_df, ts_df = load_and_prepare(input_path)
    linker = Linker([br_df, ts_df], SETTINGS, DuckDBAPI())
    train_model(linker)
    pairwise_df, clustered_df = predict_and_cluster(linker)
    save_results(linker, pairwise_df, clustered_df, output_dir)


if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    project_dir = script_dir.parent
    run(
        input_path=project_dir / "data" / "input.csv",
        output_dir=project_dir / "results",
    )
