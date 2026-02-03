# python file for calculating viability scores for TechSpeed candidates using MLflow model.
# this file is used to calculate the viability scores for the TechSpeed candidates and save the results to the int__techspeed_viability_scoring table.


import mlflow
import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    current_timestamp,
    lit,
    log,
    lower,
    monotonically_increasing_id,
    round,
    when,
)
from us import states


def _score_using_model(
    df: pd.DataFrame, modelname: str, score_col: str
) -> pd.DataFrame:
    """
    Score candidates using the latest version of the viability model.

    Args:
        df: Input DataFrame with features for scoring
        modelname: Name of the MLflow model to use for scoring
        score_col: Name of the column to store the scores

    Returns:
        DataFrame with viability scores added in the specified column
    """
    client = mlflow.tracking.MlflowClient()
    latest_model_version = max(
        client.search_model_versions(
            f"name='goodparty_data_catalog.model_predictions.{modelname}'"
        ),
        key=lambda x: x.version,
    ).version
    model = mlflow.sklearn.load_model(
        f"models:/goodparty_data_catalog.model_predictions.{modelname}/{latest_model_version}"
    )

    # Initialize score column
    df[score_col] = np.nan

    # Score only complete rows
    valid_rows = df[model.feature_names_in_].notnull().all(axis=1)
    if valid_rows.sum() > 0:
        df.loc[valid_rows, score_col] = model.predict_proba(
            df.loc[valid_rows, model.feature_names_in_]
        )[:, 1]

    return df


def _join_woe(df: DataFrame, col_name: str, spark: SparkSession) -> DataFrame:
    """
    Join Weight of Evidence (WoE) values from precomputed Delta tables.

    Args:
        df: Input DataFrame to join WoE values to
        col_name: Name of the column to apply WoE encoding to
        spark: SparkSession instance for table access

    Returns:
        DataFrame with WoE values joined for the specified column
    """
    woe_df = spark.table(
        f"goodparty_data_catalog.model_predictions.viability_br_{col_name}_woe"
    )

    # Get valid categories
    valid_cats = [
        row["cat_grouped"] for row in woe_df.select("cat_grouped").distinct().collect()
    ]
    df = df.withColumn(
        "grouped_col",
        when(col(col_name).isin(valid_cats), col(col_name)).otherwise(lit("Other")),
    )

    # Join WoE values
    df = df.join(
        woe_df.select(col("cat_grouped").alias("grouped_col"), col(f"{col_name}_woe")),
        on="grouped_col",
        how="left",
    ).drop("grouped_col")

    return df


def model(dbt, session: SparkSession) -> DataFrame:
    """
    Calculate viability scores for TechSpeed candidates using MLflow model.

    This model takes the fuzzy deduped candidacies from int__techspeed_candidates_fuzzy_deduped
    and calculates viability scores using the trained viability model.
    """
    spark = session
    # configure the data model
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="table",
        auto_liquid_cluster=True,
        tags=["intermediate", "techspeed", "viability"],
    )

    # Get the fuzzy deduped candidacies data
    df_candidates = dbt.ref("int__techspeed_candidates_fuzzy_deduped")

    # Add viability_id for tracking
    df_candidates = df_candidates.withColumn(
        "viability_id", monotonically_increasing_id()
    )

    # Select and transform columns to match viability model expectations
    df_hs = df_candidates.select(
        col("viability_id"),
        col("techspeed_candidate_code"),
        concat(col("first_name"), lit(" "), col("last_name")).alias(
            "properties_candidate_name"
        ),
        col("city").alias("properties_city"),
        col("state").alias("properties_candidate_state"),
        col("election_date").alias("properties_election_date"),
        col("office_level").alias("properties_office_level"),
        col("office_type").alias("properties_office_type"),
        when(col("election_type") == "General", "Partisan")
        .otherwise("Non-Partisan")
        .alias("properties_partisan_np"),
        col("type").alias("properties_type"),
        col("candidate_type").alias("properties_incumbent"),
        col("number_of_seats_available").alias("properties_seats_available"),
        when(col("number_of_candidates").isNull(), None)
        .when(col("number_of_candidates") == "", None)
        .otherwise(col("number_of_candidates").cast("int") - 1)
        .alias("properties_number_of_opponents"),
        when(col("uncontested") == "Uncontested", "Yes")
        .otherwise("No")
        .alias("properties_open_seat_"),
        col("party").alias("properties_candidate_party"),
        col("official_office_name").alias("properties_official_office_name"),
        col("candidate_office").alias("properties_candidate_office"),
    )

    # Create office field
    df_hs = df_hs.withColumn(
        "office",
        coalesce(
            col("properties_official_office_name"),
            col("properties_candidate_office"),
        ),
    )
    df_hs = df_hs.withColumn("office_type_new", col("properties_office_type"))

    # Create state lookup table
    state_lookup_data = [
        {"state_key": s.name.lower(), "state": s.abbr} for s in states.STATES
    ] + [{"state_key": s.abbr, "state": s.abbr} for s in states.STATES]
    state_lookup = spark.createDataFrame(pd.DataFrame(state_lookup_data))

    # Transform variables to match HubSpot format for viability model
    df_hs = (
        df_hs.withColumn(
            "n_seats",
            when(col("properties_seats_available").isNull(), None)
            .when(col("properties_seats_available") == "", None)
            .otherwise(col("properties_seats_available").cast("int")),
        )
        .withColumn("level", lower(col("properties_office_level")))
        .withColumn("state_key", lower(col("properties_candidate_state")))
        .join(state_lookup, on="state_key", how="left")
        .withColumn(
            "is_incumbent",
            when(col("properties_incumbent") == "Incumbent", True)
            .when(col("properties_incumbent") == "Challenger", False)
            .otherwise(False),  # Treat NULL/unknown as non-incumbent
        )
        .withColumn(
            "open_seat",
            when(col("properties_open_seat_") == "Yes", True)
            .when(col("properties_open_seat_") == "No", False)
            .otherwise(None),
        )
        .withColumn(
            "multi_seat",
            when(col("n_seats") > 1, 1)
            .when(col("n_seats").isNull(), None)
            .otherwise(0),
        )
        .withColumn(
            "partisan_contest",
            when(col("properties_partisan_np") == "Partisan", 1)
            .when(col("properties_partisan_np").isNull(), None)
            .otherwise(0),
        )
        .withColumn("is_unexpired", lit(False))
        .withColumn(
            "n_candidates",
            when(col("properties_number_of_opponents").isNull(), None).otherwise(
                col("properties_number_of_opponents").cast("int") + 1
            ),
        )
        .withColumn(
            "log_n_losers",
            when(col("n_candidates").isNull(), None)
            .when(col("n_seats").isNull(), None)
            .when(col("n_seats") >= col("n_candidates"), log(lit(0.001)))
            .otherwise(log(col("n_candidates") - col("n_seats"))),
        )
        .withColumnRenamed("office_type_new", "office_type")
        .withColumnRenamed("viability_id", "id")
    )

    # Apply Weight of Evidence (WoE) encoding for categorical variables
    categories_to_encode = ["state", "level", "office_type"]
    for col_name in categories_to_encode:
        df_hs = _join_woe(df_hs, col_name, spark)

    # Define features for scoring
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

    # Convert to pandas for ML model scoring
    # Disable Arrow optimization to avoid ChunkedArray conversion error
    # Keep disabled until after createDataFrame() call below
    session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
    df_toscore = df_hs.select(
        ["id", "techspeed_candidate_code"] + all_features
    ).toPandas()
    df_toscore[all_features] = df_toscore[all_features].astype(float)

    # Score using MLflow model
    # Apply viability scoring
    models_to_run = ["ViabilityWithOpponentData"]
    score_names = ["y_score0a"]

    for modelname, score_col in zip(models_to_run, score_names):
        df_toscore = _score_using_model(df_toscore, modelname, score_col)

    # Convert back to Spark DataFrame and calculate viability metrics
    df_scored = (
        session.createDataFrame(df_toscore)
        .withColumn("viability_rating_2_0", round(5 * col("y_score0a"), 2))
        .withColumn(
            "score_viability_automated",
            when(col("viability_rating_2_0").isNull(), None)
            .when(col("viability_rating_2_0") < 1, "No Chance")
            .when(col("viability_rating_2_0") < 2, "Unlikely to Win")
            .when(col("viability_rating_2_0") < 3, "Has a Chance")
            .when(col("viability_rating_2_0") < 4, "Likely to Win")
            .otherwise("Frontrunner"),
        )
    )
    # Re-enable Arrow optimization after pandas/spark conversions
    session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    # Return final result with viability scores and updated_at for incremental loading
    df_scored = df_scored.withColumn("updated_at", current_timestamp())
    return df_scored.select(
        "techspeed_candidate_code",
        "viability_rating_2_0",
        "score_viability_automated",
        "updated_at",
    )
