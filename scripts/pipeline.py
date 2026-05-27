# scripts/pipeline.py
"""
Splink entity resolution: multi-source record matching.

Usage:
    uv run python -m scripts.cli match --entity-type candidacy_stage --input data/input.csv
    uv run python -m scripts.cli match --entity-type elected_official --input catalog.schema.table
"""

import json
from pathlib import Path

import pandas as pd
from splink import Linker, SettingsCreator, block_on
from splink.internals.duckdb.database_api import DuckDBAPI

from scripts.entity_config import EntityConfig


def load_and_prepare(df: pd.DataFrame, config: EntityConfig) -> list[pd.DataFrame]:
    """Clean nulls, parse aliases, return one DataFrame per source (sorted by name)."""
    print(f"Preparing {len(df):,} rows for {config.display_name}")
    print(f"\nSource distribution:\n{df['source_name'].value_counts().to_string()}")

    for col in config.date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date.astype(str)
            df[col] = df[col].replace("NaT", None)

    # Parse JSON-array columns built by the dbt prematch model back to lists
    # so Splink's ArrayIntersectLevel can operate on them. Databricks emits
    # arrays as native lists, but cli._normalize_to_strings re-serializes them
    # to JSON to keep the DataFrame all-string.
    for col in ("first_name_aliases", "official_office_name_tokens"):
        if col in df.columns:
            df[col] = df[col].apply(
                lambda v: json.loads(v) if isinstance(v, str) else None
            )

    # Normalize nulls so Splink treats missing data correctly
    df = df.where(df.notna(), None)
    df = df.replace({"": None, "nan": None, "null": None})

    sources = sorted(df["source_name"].unique())
    source_dfs = []
    for src in sources:
        src_df = df[df["source_name"] == src].copy()
        print(f"  {src}: {len(src_df):,} records")
        source_dfs.append(src_df)

    return source_dfs


def build_settings(config: EntityConfig) -> SettingsCreator:
    """Build Splink SettingsCreator from entity config."""
    return SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        comparisons=config.comparisons,
        blocking_rules_to_generate_predictions=config.blocking_rules_for_prediction,
        retain_intermediate_calculation_columns=True,
        additional_columns_to_retain=config.additional_columns_to_retain,
    )


def train_model(linker: Linker, config: EntityConfig) -> int:
    """Estimate u via random sampling, then m via EM. Returns count of successful blocks."""
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000)

    successful_blocks = 0
    last_error = None

    for cols in config.em_training_blocks:
        try:
            linker.training.estimate_parameters_using_expectation_maximisation(
                block_on(*cols), fix_u_probabilities=True
            )
            successful_blocks += 1
        except Exception as e:
            last_error = e
            print(f"WARNING: EM training on {cols} failed: {e}")
            print("Continuing with remaining training blocks...")

    print(
        f"EM training blocks succeeded: {successful_blocks}/{len(config.em_training_blocks)}"
    )

    if successful_blocks == 0:
        raise RuntimeError(
            f"EM training failed for all {len(config.em_training_blocks)} configured blocks."
        ) from last_error

    return successful_blocks


def predict_and_cluster(
    linker: Linker, config: EntityConfig, output_dir: Path | None = None
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Predict matches, apply post-prediction filters, cluster."""
    predictions = linker.inference.predict(
        threshold_match_probability=config.predict_threshold
    )

    pred_table = predictions.physical_name
    pre_count = linker._db_api._con.execute(
        f"SELECT count(*) FROM {pred_table}"
    ).fetchone()[0]
    print(f"Pairwise predictions: {pre_count:,} pairs above {config.predict_threshold}")

    if pre_count == 0:
        print("WARNING: No predictions found.")
        return pd.DataFrame(), pd.DataFrame()

    # Apply post-prediction filters from config
    if config.post_prediction_filters:
        # Capture pre-filter pair IDs + scores for the filtered-pairs sidecar
        pre_filter_pairs = None
        if output_dir is not None:
            pre_filter_pairs = linker._db_api._con.execute(f"""
                SELECT unique_id_l, unique_id_r, match_probability, match_weight
                FROM {pred_table}
            """).fetchdf()

        combined_filter = " AND ".join(
            f"({f.strip()})" for f in config.post_prediction_filters
        )
        linker._db_api._con.execute(f"""
            CREATE OR REPLACE TABLE {pred_table} AS
            SELECT * FROM {pred_table}
            WHERE {combined_filter}
        """)

        # Write filtered-out pairs sidecar
        if output_dir is not None and pre_filter_pairs is not None:
            post_filter_pairs = linker._db_api._con.execute(f"""
                SELECT unique_id_l, unique_id_r
                FROM {pred_table}
            """).fetchdf()
            post_keys = set(
                zip(post_filter_pairs["unique_id_l"], post_filter_pairs["unique_id_r"])
            )
            filtered_mask = ~pre_filter_pairs.apply(
                lambda r: (r["unique_id_l"], r["unique_id_r"]) in post_keys, axis=1
            )
            filtered_out = pre_filter_pairs[filtered_mask].copy()

            # Canonicalize pair keys: ensure unique_id_l < unique_id_r
            swap = filtered_out["unique_id_l"] > filtered_out["unique_id_r"]
            filtered_out.loc[swap, ["unique_id_l", "unique_id_r"]] = filtered_out.loc[
                swap, ["unique_id_r", "unique_id_l"]
            ].values
            filtered_out = filtered_out.rename(
                columns={
                    "match_probability": "match_probability_pre_filter",
                    "match_weight": "match_weight_pre_filter",
                }
            )
            filtered_out[
                [
                    "unique_id_l",
                    "unique_id_r",
                    "match_probability_pre_filter",
                    "match_weight_pre_filter",
                ]
            ].to_csv(output_dir / "filtered_pairs.csv", index=False)

    pairwise_df = predictions.as_pandas_dataframe()
    dropped = pre_count - len(pairwise_df)
    if dropped > 0:
        print(f"Post-prediction filters: removed {dropped:,} pairs")

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions, threshold_match_probability=config.cluster_threshold
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
    config: EntityConfig,
) -> None:
    """Write CSVs and diagnostic charts."""

    import numpy as np

    def to_json(v):
        # Strings pass through unchanged (already JSON-serialized in a prior run)
        if isinstance(v, str):
            return v
        if isinstance(v, list):
            return json.dumps(v)
        if isinstance(v, np.ndarray):
            return json.dumps(v.tolist())
        # Anything else (None, NaN, numpy scalar in a list cell) → empty array sentinel.
        # Strict ndarray check above (vs hasattr "tolist") is required so np.float64(nan)
        # cells inside a list column don't json.dumps() to the non-standard "NaN" token.
        return "[]"

    def _is_list_col(series: pd.Series) -> bool:
        # Probe the first non-null value — JSON-serialize only true list/array
        # columns. Required because Delta CREATE TABLE maps object→STRING but
        # pyarrow infers list<...> from list values, mismatching the schema.
        # Strict isinstance check (no hasattr 'tolist') avoids matching numpy
        # scalars like np.float64, which also expose tolist() but must stay
        # numeric in the output.
        idx = series.first_valid_index()
        if idx is None:
            return False
        sample = series.loc[idx]
        return isinstance(sample, (list, np.ndarray))

    for df in (pairwise_df, clustered_df):
        if len(df) == 0:
            continue
        for col in df.columns:
            if _is_list_col(df[col]):
                df[col] = df[col].apply(to_json)

    pairwise_df.to_csv(output_dir / "pairwise_predictions.csv", index=False)
    if len(clustered_df) > 0:
        clustered_df.to_csv(output_dir / config.clustered_output_name, index=False)

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


def run(
    input_df: pd.DataFrame, output_dir: Path, config: EntityConfig
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare data, train, predict, cluster, save. Returns (pairwise_df, clustered_df)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    source_dfs = load_and_prepare(input_df, config)
    settings = build_settings(config)
    linker = Linker(source_dfs, settings, DuckDBAPI())
    train_model(linker, config)
    pairwise_df, clustered_df = predict_and_cluster(linker, config, output_dir)
    save_results(linker, pairwise_df, clustered_df, output_dir, config)
    return pairwise_df, clustered_df
