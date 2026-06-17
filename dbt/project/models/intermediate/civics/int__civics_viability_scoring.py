"""
dbt Python model: int__civics_viability_scoring

Scores all candidacies in the civics mart using the viability MLflow model
waterfall. Produces a score for every candidacy that has enough data to
run at least the most-permissive model.

Waterfall (best available wins via COALESCE):
  1. ViabilityWithOpponentData    — all 9 features present
  2. ViabilityWithoutOpenSeat     — missing open_seat
  3. ViabilityNoOpponentData      — missing log_n_losers / multi_seat
  4. ViabilityNoIncumbency        — missing is_incumbent
  5. ViabilityNoCandidateDataHS   — most permissive (office type + location only)

Output key: gp_candidacy_id
Joins to mart output: mart_civics.candidacy.viability_score (via int__civics_candidacy_*)
"""

import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    lit,
    log,
    lower,
    round,
    when,
)
from us import states


def _score_using_model(df: pd.DataFrame, modelname: str, score_col: str) -> pd.DataFrame:
    client = mlflow.tracking.MlflowClient()
    model = mlflow.sklearn.load_model(
        f"models:/goodparty_data_catalog.model_predictions.{modelname}@prod"
    )
    df[score_col] = np.nan
    valid_rows = df[model.feature_names_in_].notnull().all(axis=1)
    if valid_rows.sum() > 0:
        df.loc[valid_rows, score_col] = model.predict_proba(
            df.loc[valid_rows, model.feature_names_in_]
        )[:, 1]
    return df


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

    candidacy = dbt.ref("m_civics__candidacy")
    election   = dbt.ref("m_civics__election")

    # bring in seats_available and number_of_opponents from the election table
    election_fields = election.select(
        col("gp_election_id"),
        col("seats_available"),
        col("number_of_opponents").alias("raw_n_opponents"),
    )

    df = candidacy.join(election_fields, on="gp_election_id", how="left")

    # state normalisation — handles both full names and abbreviations
    state_lookup = spark.createDataFrame(
        pd.DataFrame(
            [{"state_key": s.name.lower(), "state": s.abbr} for s in states.STATES]
            + [{"state_key": s.abbr.lower(), "state": s.abbr} for s in states.STATES]
        )
    )

    df = (
        df
        .withColumn("state_key", lower(col("state")))
        .drop("state")
        .join(state_lookup, on="state_key", how="left")
        .withColumn("level", lower(col("office_level")))

        # seats
        .withColumn(
            "n_seats",
            when(col("seats_available").isNull(), None)
            .when(col("seats_available") == 0, None)
            .otherwise(col("seats_available").cast("int")),
        )

        # n_candidates from opponents field ("10+" → 11, empty → null)
        .withColumn(
            "n_candidates",
            when(col("raw_n_opponents") == "10+", lit(11))
            .when(col("raw_n_opponents").isNull() | (col("raw_n_opponents") == ""), None)
            .otherwise(col("raw_n_opponents").cast("int") + 1),
        )

        # derived features
        .withColumn(
            "multi_seat",
            when(col("n_seats").isNull(), None)
            .when(col("n_seats") > 1, lit(1))
            .otherwise(lit(0)),
        )
        .withColumn(
            "partisan_contest",
            when(col("is_partisan").isNull(), None)
            .when(col("is_partisan"), lit(1))
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
        .withColumnRenamed("is_open_seat",    "open_seat")
        .withColumnRenamed("is_incumbent",    "is_incumbent")
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
        ("viabilitywithopponentdata",  "y_score0a"),
        ("viabilitywithoutopenseat",   "y_score0b"),
        ("viabilitynoopponentdata",    "y_score2"),
        ("viabilitynoincumbency",      "y_score3"),
        ("viabilitynocandidatedatahs", "y_score1a"),
    ]
    for modelname, score_col in waterfall:
        df_pd = _score_using_model(df_pd, modelname, score_col)

    df_scored = (
        spark.createDataFrame(df_pd.to_dict("records"))
        .withColumn(
            "viability_rating_2_0",
            round(
                5 * coalesce(
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

    return df_scored.select(
        "gp_candidacy_id",
        "viability_rating_2_0",
        "score_viability_automated",
        "updated_at",
    )
