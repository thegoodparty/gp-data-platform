# dbt configuration
def dbt_config():
    return {
        "materialized": "table",
        "tags": ["intermediate", "ballotready", "viability"],
        "submission_method": "serverless_cluster",
        "create_notebook": True,
    }


from pyspark.sql import DataFrame, SparkSession
...
def model(dbt, session: SparkSession) -> DataFrame:
    """
    Calculate viability scores for BallotReady candidates using MLflow model.

    This model takes the final filtered candidacies from int__ballotready_final_candidacies
    and calculates viability scores using the trained viability model.
    """
    import mlflow
    import numpy as np
    import pandas as pd
    import pyspark.sql.functions as F
    from us import states

    # Get the final candidacies data
    df_candidates = dbt.ref("int__ballotready_final_candidacies")

    # Add viability_id for tracking
    df_candidates = df_candidates.withColumn(
        "viability_id", F.monotonically_increasing_id()
    )

    # Select and transform columns to match viability model expectations
    df_hs = df_candidates.select(
        F.col("viability_id"),
        F.col("br_candidate_code"),
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias(
            "properties_candidate_name"
        ),
        F.col("city").alias("properties_city"),
        F.col("state").alias("properties_candidate_state"),
        F.col("election_date").alias("properties_election_date"),
        F.col("office_level").alias("properties_office_level"),
        F.col("office_type").alias("properties_office_type"),
        F.when(F.col("election_type") == "General", "Partisan")
        .otherwise("Non-Partisan")
        .alias("properties_partisan_np"),
        F.col("type").alias("properties_type"),
        F.when(F.col("type") == "incumbent", "Incumbent")
        .when(F.col("type") == "challenger", "Challenger")
        .otherwise("Open")
        .alias("properties_incumbent"),
        F.col("number_of_seats_available").alias("properties_seats_available"),
        F.when(F.col("number_of_candidates").isNull(), None)
        .when(F.col("number_of_candidates") == "", None)
        .otherwise(F.col("number_of_candidates").cast("int") - 1)
        .alias("properties_number_of_opponents"),
        F.when(F.col("uncontested"), "Yes")
        .otherwise("No")
        .alias("properties_open_seat_"),
        F.col("party_affiliation").alias("properties_candidate_party"),
        F.col("official_office_name").alias("properties_official_office_name"),
        F.col("candidate_office").alias("properties_candidate_office"),
    )

    # Create office field
    df_hs = df_hs.withColumn(
        "office",
        F.coalesce(
            F.col("properties_official_office_name"),
            F.col("properties_candidate_office"),
        ),
    )
    df_hs = df_hs.withColumn("office_type_new", F.col("properties_office_type"))

    # Create state lookup table
    state_lookup_data = [
        {"state_key": s.name.lower(), "state": s.abbr} for s in states.STATES
    ] + [{"state_key": s.abbr, "state": s.abbr} for s in states.STATES]
    state_lookup = spark.createDataFrame(pd.DataFrame(state_lookup_data))

    # Transform variables to match HubSpot format for viability model
    df_hs = (
        df_hs.withColumn(
            "n_seats",
            F.when(F.col("properties_seats_available").isNull(), None)
            .when(F.col("properties_seats_available") == "", None)
            .otherwise(F.col("properties_seats_available").cast("int")),
        )
        .withColumn("level", F.lower(F.col("properties_office_level")))
        .withColumn("state_key", F.lower(F.col("properties_candidate_state")))
        .join(state_lookup, on="state_key", how="left")
        .withColumn(
            "is_incumbent",
            F.when(F.col("properties_incumbent") == "Incumbent", True)
            .when(F.col("properties_incumbent") == "Challenger", False)
            .otherwise(None),
        )
        .withColumn(
            "open_seat",
            F.when(F.col("properties_open_seat_") == "Yes", True)
            .when(F.col("properties_open_seat_") == "No", False)
            .otherwise(None),
        )
        .withColumn(
            "multi_seat",
            F.when(F.col("n_seats") > 1, 1)
            .when(F.col("n_seats").isNull(), None)
            .otherwise(0),
        )
        .withColumn(
            "partisan_contest",
            F.when(F.col("properties_partisan_np") == "Partisan", 1)
            .when(F.col("properties_partisan_np").isNull(), None)
            .otherwise(0),
        )
        .withColumn("is_unexpired", F.lit(False))
        .withColumn(
            "n_candidates",
            F.when(F.col("properties_number_of_opponents").isNull(), None).otherwise(
                F.col("properties_number_of_opponents").cast("int") + 1
            ),
        )
        .withColumn(
            "log_n_losers",
            F.when(F.col("n_candidates").isNull(), None)
            .when(F.col("n_seats").isNull(), None)
            .when(F.col("n_seats") >= F.col("n_candidates"), F.log(F.lit(0.001)))
            .otherwise(F.log(F.col("n_candidates") - F.col("n_seats"))),
        )
        .withColumnRenamed("office_type_new", "office_type")
        .withColumnRenamed("viability_id", "id")
    )

    # Join WoE (Weight of Evidence) values from pre-computed tables
    def join_woe(df, col):
        """Join WoE values from precomputed Delta tables"""
        woe_df = spark.table(f"sandbox.viability_br_{col}_woe")

        # Get valid categories
        valid_cats = [
            row["cat_grouped"]
            for row in woe_df.select("cat_grouped").distinct().collect()
        ]
        df = df.withColumn(
            "grouped_col",
            F.when(F.col(col).isin(valid_cats), F.col(col)).otherwise(F.lit("Other")),
        )

        # Join WoE values
        df = df.join(
            woe_df.select(
                F.col("cat_grouped").alias("grouped_col"), F.col(f"{col}_woe")
            ),
            on="grouped_col",
            how="left",
        ).drop("grouped_col")

        return df

    # Apply WoE encoding for categorical variables
    categories_to_encode = ["state", "level", "office_type"]
    for col in categories_to_encode:
        df_hs = join_woe(df_hs, col)

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
    df_toscore = df_hs.select(["id", "br_candidate_code"] + all_features).toPandas()
    df_toscore[all_features] = df_toscore[all_features].astype(float)

    # Score using MLflow model
    def score_using_model(df, modelname, score_col):
        """Score candidates using the latest version of the viability model"""
        client = mlflow.tracking.MlflowClient()
        latest_model_version = max(
            client.search_model_versions(
                f"name='goodparty_data_catalog.sandbox.{modelname}'"
            ),
            key=lambda x: x.version,
        ).version
        model = mlflow.sklearn.load_model(
            f"models:/goodparty_data_catalog.sandbox.{modelname}/{latest_model_version}"
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

    # Apply viability scoring
    models_to_run = ["ViabilityWithOpponentData"]
    score_names = ["y_score0a"]

    for modelname, score_col in zip(models_to_run, score_names):
        df_toscore = score_using_model(df_toscore, modelname, score_col)

    # Convert back to Spark DataFrame and calculate viability metrics
    df_scored = (
        spark.createDataFrame(df_toscore)
        .withColumn("viability_rating_2_0", F.round(5 * F.col("y_score0a"), 2))
        .withColumn(
            "score_viability_automated",
            F.when(F.col("viability_rating_2_0").isNull(), None)
            .when(F.col("viability_rating_2_0") < 1, "No Chance")
            .when(F.col("viability_rating_2_0") < 2, "Unlikely to Win")
            .when(F.col("viability_rating_2_0") < 3, "Has a Chance")
            .when(F.col("viability_rating_2_0") < 4, "Likely to Win")
            .otherwise("Frontrunner"),
        )
    )

    # Return final result with viability scores
    return df_scored.select(
        "br_candidate_code", "viability_rating_2_0", "score_viability_automated"
    )
