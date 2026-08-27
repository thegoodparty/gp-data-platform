# scripts/pipeline.py
"""
Splink entity resolution: multi-source record matching.

Usage:
    uv run python -m scripts.cli match --entity-type candidacy_stage --input data/input.csv
    uv run python -m scripts.cli match --entity-type elected_official --input catalog.schema.table
"""

import json
import os
import re
from pathlib import Path

import duckdb
import pandas as pd
from splink import Linker, SettingsCreator, block_on
from splink.internals.duckdb.database_api import DuckDBAPI
from splink.internals.pipeline import CTEPipeline
from splink.internals.vertically_concatenate import compute_df_concat_with_tf

from scripts.entity_config import EntityConfig


def _duckdb_api() -> DuckDBAPI:
    """DuckDBAPI honoring an optional MATCHA_DUCKDB_MEMORY_LIMIT (e.g. "12GB").

    When set, DuckDB spills oversized frames to disk at the cap instead of
    pressuring the whole machine. Unset keeps DuckDB's default (80% of RAM),
    which is correct in the memory-limited production container.
    """
    limit = os.environ.get("MATCHA_DUCKDB_MEMORY_LIMIT")
    if not limit:
        return DuckDBAPI()
    try:
        return DuckDBAPI(connection=duckdb.connect(config={"memory_limit": limit}))
    except duckdb.Error as e:
        raise ValueError(f"Invalid MATCHA_DUCKDB_MEMORY_LIMIT={limit!r}: {e}") from e


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
    for col in (
        "first_name_aliases",
        "first_name_tokens",
        "official_office_name_tokens",
        "matched_candidacy_stage_clusters",
    ):
        if col in df.columns:
            df[col] = df[col].apply(lambda v: json.loads(v) if isinstance(v, str) else None)

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
        link_type=config.link_type,
        unique_id_column_name="unique_id",
        comparisons=config.comparisons,
        blocking_rules_to_generate_predictions=config.blocking_rules_for_prediction,
        retain_intermediate_calculation_columns=True,
        additional_columns_to_retain=config.additional_columns_to_retain,
    )


def _inject_deterministic_group_edges(linker: Linker, config: EntityConfig, pred_table: str) -> int:
    """Add same-group edges to the predictions table at p=1.0.

    Deterministic identity (shared native ids) must survive probabilistic
    scoring: a pair the dbt graph already resolved has to cluster even when its
    attributes disagree. One hub-to-minimum star edge per group is enough,
    because clustering is transitive.

    Scoring a same-group pair is not enough on its own. Splink scores plenty of
    them below the cluster threshold — a BallotReady and a TechSpeed record for
    one person, agreeing on nothing but the name, lands around 0.45 — so every
    same-group pair is raised to 1.0, not just the ones with no row yet. Leaving
    the scored ones alone splits the group. match_weight is left as Splink fit
    it, so the original score stays recoverable.

    Injected rows carry the same _l/_r columns Splink emits, so the audits and
    every downstream consumer see one row shape. Only the gammas, match_weight,
    and match_key stay NULL: those pairs were asserted, not scored, and no
    blocking rule produced them.
    """
    col = config.deterministic_grouping_column
    if not col:
        return 0

    concat = compute_df_concat_with_tf(linker, CTEPipeline())
    con = linker._db_api._con

    promoted = con.execute(f"""
        UPDATE {pred_table} SET match_probability = 1.0
        WHERE "{col}_l" IS NOT NULL AND "{col}_l" = "{col}_r" AND match_probability < 1.0
    """).fetchone()[0]

    pred_columns = {d[0] for d in con.execute(f"SELECT * FROM {pred_table} LIMIT 0").description}
    sides = [
        expr
        for c in concat.columns
        if f"{c.unquote().name}_l" in pred_columns
        for expr in (f"hub.{c.name} AS {c.name_l}", f"spoke.{c.name} AS {c.name_r}")
    ]

    # Match the existing pair keys with least/greatest rather than testing both
    # orderings with an OR: the OR form cannot be decorrelated, and DuckDB falls
    # back to a nested loop over the whole predictions table.
    inserted = con.execute(f"""
        INSERT INTO {pred_table} BY NAME
        WITH spokes AS (
            SELECT *, min(unique_id) OVER (PARTITION BY "{col}") AS hub_id
            FROM {concat.physical_name}
            WHERE "{col}" IS NOT NULL
            QUALIFY hub_id <> unique_id
        )
        SELECT {", ".join(sides)}, 1.0 AS match_probability
        FROM spokes AS spoke
        JOIN {concat.physical_name} AS hub ON hub.unique_id = spoke.hub_id
        WHERE NOT EXISTS (
            SELECT 1 FROM {pred_table} AS p
            WHERE least(p.unique_id_l, p.unique_id_r) = spoke.hub_id
              AND greatest(p.unique_id_l, p.unique_id_r) = spoke.unique_id
        )
    """).fetchone()[0]

    print(f"Deterministic {col} edges: {inserted:,} injected, {promoted:,} scored pairs raised to 1.0")
    return inserted + promoted


def train_model(linker: Linker, config: EntityConfig) -> int:
    """Estimate u via random sampling, then m via EM. Returns count of successful blocks."""
    # Seeded so a rerun on unchanged input reproduces the same clusters.
    # Unseeded, u sampling moved 451 of 986,360 person records between clusters
    # across two runs, and published ids are minted from cluster membership.
    linker.training.estimate_u_using_random_sampling(max_pairs=5_000_000, seed=20260827)

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

    print(f"EM training blocks succeeded: {successful_blocks}/{len(config.em_training_blocks)}")

    if successful_blocks == 0:
        raise RuntimeError(
            f"EM training failed for all {len(config.em_training_blocks)} configured blocks."
        ) from last_error

    # Some comparisons get their m estimates from a single training block
    # (e.g. candidacy's election_date only trains in the email session; every
    # other block references election_date). If that one block failed above,
    # the comparison ships with no m at all and scores near the cluster
    # threshold are silently miscalibrated — fail hard instead of warning.
    # Tolerated: a comparison referenced by every EM block can never train m
    # (a deliberate config choice, e.g. election_stage blocks on state
    # everywhere and its post-filter reads raw _l/_r columns); sparse or
    # all-NULL columns on small cohorts (only enforced when a block failed).
    if successful_blocks < len(config.em_training_blocks):
        trainable = {
            c.output_column_name
            for c in linker._settings_obj.comparisons
            if any(all(c.output_column_name not in col for col in cols) for cols in config.em_training_blocks)
        }
        untrained = [
            c.output_column_name
            for c in linker._settings_obj.comparisons
            if c.output_column_name in trainable and not c._some_m_are_trained
        ]
        if untrained:
            raise RuntimeError(
                f"EM training left comparisons with no m estimates: {untrained}. "
                "The failed training block was their only coverage."
            ) from last_error

    return successful_blocks


def predict_and_cluster(
    linker: Linker, config: EntityConfig
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Predict matches, apply post-prediction filters, cluster.

    Returns the pairwise, clustered, and filtered-out frames. The caller writes
    them; nothing here touches the filesystem.
    """
    predictions = linker.inference.predict(threshold_match_probability=config.predict_threshold)

    pred_table = predictions.physical_name
    pre_count = linker._db_api._con.execute(f"SELECT count(*) FROM {pred_table}").fetchone()[0]
    print(f"Pairwise predictions: {pre_count:,} pairs above {config.predict_threshold}")

    if pre_count == 0:
        print("WARNING: No predictions found.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Injection runs first because the filters below exempt deterministic pairs
    # outright. That keeps the kept and filtered sets exact complements of one
    # predicate, and an exempted pair keeps the gammas Splink computed for it.
    _inject_deterministic_group_edges(linker, config, pred_table)

    filtered_df = pd.DataFrame()
    if config.post_prediction_filters:
        # Splink drops gamma_<col> from the prediction frame when a comparison
        # is never trained — e.g. a column used only as an exact-equality
        # blocking key (m never estimated) or one that is NULL across the whole
        # input. A post-prediction filter that references such a gamma column
        # would either raise a DuckDB binder error or, worse, silently skip the
        # guard and over-match. Fail loudly instead: the fix is to reference the
        # retained raw _l/_r columns (see ELECTION_STAGE_POST_PREDICTION_FILTER).
        available_cols = {
            d[0] for d in linker._db_api._con.execute(f"SELECT * FROM {pred_table} LIMIT 0").description
        }
        for f in config.post_prediction_filters:
            missing = sorted(set(re.findall(r"\bgamma_\w+", f)) - available_cols)
            if missing:
                raise ValueError(
                    "Post-prediction filter references gamma column(s) absent "
                    f"from the prediction frame: {missing}. Splink drops gamma "
                    "columns for untrained comparisons (blocking-only or "
                    "all-NULL columns); reference the raw _l/_r columns instead."
                )

        keep = " AND ".join(f"({f.strip()})" for f in config.post_prediction_filters)
        if col := config.deterministic_grouping_column:
            keep = f'("{col}_l" IS NOT NULL AND "{col}_l" = "{col}_r") OR ({keep})'
        # coalesce so a NULL predicate lands in exactly one of the two sets
        # rather than being dropped from both.
        keep = f"coalesce({keep}, false)"

        filtered_df = linker._db_api._con.execute(f"""
            SELECT
                least(unique_id_l, unique_id_r) AS unique_id_l,
                greatest(unique_id_l, unique_id_r) AS unique_id_r,
                match_probability AS match_probability_pre_filter,
                match_weight AS match_weight_pre_filter
            FROM {pred_table} WHERE NOT {keep}
        """).fetchdf()

        linker._db_api._con.execute(f"""
            CREATE OR REPLACE TABLE {pred_table} AS
            SELECT * FROM {pred_table} WHERE {keep}
        """)

        if len(filtered_df):
            print(f"Post-prediction filters: removed {len(filtered_df):,} pairs")

    pairwise_df = predictions.as_pandas_dataframe()

    clusters = linker.clustering.cluster_pairwise_predictions_at_threshold(
        predictions, threshold_match_probability=config.cluster_threshold
    )
    clustered_df = clusters.as_pandas_dataframe()

    n_matched = (clustered_df.groupby("cluster_id").size() > 1).sum()
    n_cross = (clustered_df.groupby("cluster_id")["source_dataset"].nunique() > 1).sum()
    print(f"Matched clusters: {n_matched:,}  |  Cross-source: {n_cross:,}")
    if (within := n_matched - n_cross) > 0:
        prefix = "" if config.expects_within_source_duplicates else "WARNING: "
        print(f"{prefix}{within} within-source duplicate clusters found")

    return pairwise_df, clustered_df, filtered_df


def save_results(
    linker: Linker,
    pairwise_df: pd.DataFrame,
    clustered_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
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
    if config.post_prediction_filters:
        filtered_df.to_csv(output_dir / "filtered_pairs.csv", index=False)

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


def run(input_df: pd.DataFrame, output_dir: Path, config: EntityConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare data, train, predict, cluster, save. Returns (pairwise_df, clustered_df)."""
    output_dir.mkdir(parents=True, exist_ok=True)
    source_dfs = load_and_prepare(input_df, config)
    settings = build_settings(config)
    linker = Linker(source_dfs, settings, _duckdb_api())
    train_model(linker, config)
    pairwise_df, clustered_df, filtered_df = predict_and_cluster(linker, config)
    save_results(linker, pairwise_df, clustered_df, filtered_df, output_dir, config)
    return pairwise_df, clustered_df
