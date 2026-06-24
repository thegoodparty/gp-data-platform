"""
dbt Python model: int__civics_viability_scoring

Scores all candidacies in the civics mart using the viability MLflow model
waterfall. Produces a score for every candidacy that has enough data to
run at least the most-permissive model.

Waterfall (best available wins via COALESCE):
  1. ViabilityWithOpponentData    — all 9 features
  2. ViabilityWithoutOpenSeat     — drops open_seat
  3. ViabilityNoOpponentData      — drops open_seat + log_n_losers
  4. ViabilityNoIncumbency        — drops open_seat + is_incumbent
  5. ViabilityNoCandidateDataHS   — most resilient to missingness: multi_seat, partisan_contest, is_unexpired, office_type_woe, state_woe, level_woe

Output key: gp_candidacy_id
Joins to mart output: mart_civics.candidacy.viability_score (via int__civics_candidacy_*)
"""

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    current_timestamp,
    isnan,
    lit,
    log,
    lower,
    round,
    when,
)


def _resolve_latest_version(modelname: str) -> str:
    """Latest registered version of an MLflow model in model_predictions.

    Mirrors the sibling int__techspeed_viability_scoring.py, but sorts versions
    NUMERICALLY: MLflow's ModelVersion.version is a string, so the sibling's
    max(..., key=lambda x: x.version) picks the wrong version once a model
    reaches v10 ("9" > "10" lexicographically) -- viabilitywithopponentdata is
    already at v5. No @prod alias is used: these models load by latest version
    because setting @prod needs apply_tag, which is gated by design.
    """
    client = mlflow.tracking.MlflowClient()
    versions = client.search_model_versions(f"name='goodparty_data_catalog.model_predictions.{modelname}'")
    if not versions:
        raise ValueError(
            f"No registered versions found for goodparty_data_catalog.model_predictions.{modelname}; "
            "the model must be copied into model_predictions before this scorer runs."
        )
    return max(versions, key=lambda mv: int(mv.version)).version


def _score_using_model(df: pd.DataFrame, modelname: str, score_col: str) -> tuple[pd.DataFrame, str]:
    version = _resolve_latest_version(modelname)
    model = mlflow.sklearn.load_model(
        f"models:/goodparty_data_catalog.model_predictions.{modelname}/{version}"
    )
    # Subset preflight: the fallback models intentionally use fewer than the 9
    # inputs, so require the model's features to be a SUBSET of what we provide
    # (an exact-9 check would wrongly reject valid fallbacks). A missing feature
    # would otherwise surface as an opaque KeyError on the df[...] selection.
    missing = set(model.feature_names_in_) - set(df.columns)
    if missing:
        raise KeyError(f"{modelname} needs features absent from the scoring frame: {sorted(missing)}")
    df[score_col] = np.nan
    valid_rows = df[model.feature_names_in_].notnull().all(axis=1)
    if valid_rows.sum() > 0:
        df.loc[valid_rows, score_col] = model.predict_proba(df.loc[valid_rows, model.feature_names_in_])[:, 1]
    return df, version


def _join_woe(df: DataFrame, col_name: str, spark: SparkSession) -> DataFrame:
    woe_df = spark.table(f"goodparty_data_catalog.model_predictions.viability_br_{col_name}_woe")
    valid_cats = [r["cat_grouped"] for r in woe_df.select("cat_grouped").distinct().collect()]
    df = df.withColumn(
        "grouped_col",
        when(col(col_name).isin(valid_cats), col(col_name)).otherwise(lit("Other")),
    )
    df = df.join(
        woe_df.select(col("cat_grouped").alias("grouped_col"), col(f"{col_name}_woe")),
        on="grouped_col",
        how="left",
    ).drop("grouped_col")
    return df


def model(dbt, session: SparkSession) -> DataFrame:
    spark = session

    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="table",
        auto_liquid_cluster=True,
        tags=["intermediate", "civics", "viability"],
    )

    candidacy = dbt.ref("candidacy")
    election = dbt.ref("election")

    # bring in state, seats_available, and number_of_opponents fallback from election table
    # (state is not on the candidacy mart; election.state is already a 2-letter abbreviation)
    election_fields = election.select(
        col("gp_election_id"),
        col("state"),
        col("seats_available"),
        col("number_of_opponents").alias("raw_n_opponents"),
    )

    # count candidacies per election from the mart (~75% fill vs ~2.6% for the pre-computed column)
    # only trust this count when > 1: a count of 1 may mean we only ingested one candidate,
    # not that the race is genuinely uncontested
    n_candidates_per_election = (
        candidacy.filter(col("gp_election_id").isNotNull())
        .groupBy("gp_election_id")
        .agg(count("*").alias("n_candidates_mart"))
    )

    df = candidacy.join(election_fields, on="gp_election_id", how="left").join(
        n_candidates_per_election, on="gp_election_id", how="left"
    )

    df = (
        df.withColumn("level", lower(col("office_level")))
        # seats
        .withColumn(
            "n_seats",
            when(col("seats_available").isNull(), None)
            .when(col("seats_available") == 0, None)
            .otherwise(col("seats_available").cast("int")),
        )
        # n_candidates: trust mart count when > 1, else fall back to election column, else null
        .withColumn(
            "n_candidates",
            when(col("n_candidates_mart") > 1, col("n_candidates_mart"))
            .when(
                col("raw_n_opponents").isNotNull() & (col("raw_n_opponents") != ""),
                when(col("raw_n_opponents") == "10+", lit(11)).otherwise(
                    col("raw_n_opponents").cast("int") + 1
                ),
            )
            .otherwise(None),
        )
        # derived features
        .withColumn(
            "multi_seat",
            when(col("n_seats").isNull(), None).when(col("n_seats") > 1, lit(1)).otherwise(lit(0)),
        )
        .withColumn(
            "partisan_contest",
            when(col("is_partisan").isNotNull(), when(col("is_partisan"), lit(1)).otherwise(lit(0)))
            .when(col("level").isin("federal", "state"), lit(1))
            .otherwise(lit(0)),
        )
        .withColumn("is_unexpired", lit(False))
        .withColumn(
            "log_n_losers",
            when(col("n_candidates").isNull() | col("n_seats").isNull(), None)
            .when(col("n_seats") >= col("n_candidates"), log(lit(0.001)))
            .otherwise(log(col("n_candidates") - col("n_seats"))),
        )
        # open_seat: boolean → keep as-is (pandas will cast to float 0/1/NaN)
        .withColumnRenamed("is_open_seat", "open_seat")
    )

    # WoE encoding
    for c in ["state", "level", "office_type"]:
        df = _join_woe(df, c, spark)

    all_features = [
        "multi_seat",
        "partisan_contest",
        "is_unexpired",
        "office_type_woe",
        "state_woe",
        "level_woe",
        "is_incumbent",
        "open_seat",
        "log_n_losers",
    ]

    df_pd = df.select(["gp_candidacy_id", *all_features]).toPandas()
    df_pd[all_features] = df_pd[all_features].astype(float)

    # waterfall — each model scores the rows it can; COALESCE picks best available
    waterfall = [
        ("viabilitywithopponentdata", "y_score0a"),
        ("viabilitywithoutopenseat", "y_score0b"),
        ("viabilitynoopponentdata", "y_score2"),
        ("viabilitynoincumbency", "y_score3"),
        ("viabilitynocandidatedatahs", "y_score1a"),
    ]
    resolved_versions: dict[str, str] = {}
    for modelname, score_col in waterfall:
        df_pd, resolved_versions[modelname] = _score_using_model(df_pd, modelname, score_col)

    score_cols = ["y_score0a", "y_score0b", "y_score2", "y_score3", "y_score1a"]

    df_scored = spark.createDataFrame(df_pd.to_dict("records"))

    # pandas NaN survives as float NaN in Spark (not null) — convert explicitly
    # so COALESCE and comparisons behave correctly
    for s in score_cols:
        df_scored = df_scored.withColumn(s, when(col(s).isNull() | isnan(col(s)), None).otherwise(col(s)))

    df_scored = (
        df_scored.withColumn(
            "viability_rating_2_0",
            round(
                5
                * coalesce(
                    col("y_score0a"),
                    col("y_score0b"),
                    col("y_score2"),
                    col("y_score3"),
                    col("y_score1a"),
                ),
                2,
            ),
        )
        .withColumn(
            "score_viability_automated",
            when(col("viability_rating_2_0").isNull(), None)
            .when(col("viability_rating_2_0") < 1, "No Chance")
            .when(col("viability_rating_2_0") < 2, "Unlikely to Win")
            .when(col("viability_rating_2_0") < 3, "Has a Chance")
            .when(col("viability_rating_2_0") < 4, "Likely to Win")
            .otherwise("Frontrunner"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    # Per-row provenance: which waterfall model produced the surviving (coalesced)
    # score, and that model's resolved version. The waterfall order IS the coalesce
    # order, so the first non-null score column is the winning model. Build the
    # when-chain back-to-front so the earliest model is the outermost (highest
    # priority) branch, matching the coalesce above. Internal traceability + a
    # code-pin rollback target; NOT surfaced on candidacy_scored (sales is
    # provenance-free by decision).
    waterfall_provenance = [
        ("y_score0a", "viabilitywithopponentdata"),
        ("y_score0b", "viabilitywithoutopenseat"),
        ("y_score2", "viabilitynoopponentdata"),
        ("y_score3", "viabilitynoincumbency"),
        ("y_score1a", "viabilitynocandidatedatahs"),
    ]
    scoring_model_expr = lit(None).cast("string")
    model_version_expr = lit(None).cast("string")
    for prov_score_col, prov_modelname in reversed(waterfall_provenance):
        scoring_model_expr = when(col(prov_score_col).isNotNull(), lit(prov_modelname)).otherwise(
            scoring_model_expr
        )
        model_version_expr = when(
            col(prov_score_col).isNotNull(), lit(resolved_versions[prov_modelname])
        ).otherwise(model_version_expr)
    df_scored = df_scored.withColumn("scoring_model", scoring_model_expr).withColumn(
        "model_version", model_version_expr
    )

    return df_scored.select(
        "gp_candidacy_id",
        "viability_rating_2_0",
        "score_viability_automated",
        "scoring_model",
        "model_version",
        "updated_at",
    )
