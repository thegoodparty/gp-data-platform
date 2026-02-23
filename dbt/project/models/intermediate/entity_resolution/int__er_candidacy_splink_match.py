import subprocess
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.window import Window

# Install splink at runtime if not available
try:
    import splink  # noqa: F401
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "splink"])


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="table",
        auto_liquid_cluster=True,
        tags=["intermediate", "entity_resolution"],
    )

    # Load inputs
    features: DataFrame = dbt.ref("int__er_match_features_union")
    deterministic_matches: DataFrame = dbt.ref("int__er_deterministic_match")

    # Collect already-matched record IDs
    matched_ts = deterministic_matches.select("ts_record_id").distinct()
    matched_br = deterministic_matches.select("br_record_id").distinct()

    # Filter to unmatched records only
    ts_unmatched = features.filter(col("source_system") == "techspeed").join(
        matched_ts, features["record_id"] == matched_ts["ts_record_id"], "left_anti"
    )
    br_unmatched = features.filter(col("source_system") == "ballotready").join(
        matched_br, features["record_id"] == matched_br["br_record_id"], "left_anti"
    )

    # If either side is empty, return empty DataFrame
    if ts_unmatched.count() == 0 or br_unmatched.count() == 0:
        return session.createDataFrame(
            [],
            schema="ts_record_id string, br_record_id string, "
            "splink_match_probability double, splink_match_weight double, "
            "decided_by string",
        )

    # Import Splink (must be installed on the cluster)
    import splink.comparison_library as cl
    from splink import DuckDBAPI, Linker, SettingsCreator, block_on

    # Prepare DataFrames for Splink — convert to pandas for DuckDB backend
    # Add source tag to distinguish sides
    ts_pdf = (
        ts_unmatched.select(
            col("record_id").alias("unique_id"),
            "first_name_clean",
            "last_name_clean",
            "state_abbr",
            "election_date",
            "election_year",
            "candidate_office_clean",
            "office_type_clean",
            "email_clean",
            "phone_clean",
            "city_clean",
            "district_clean",
        )
        .withColumn("source_dataset", expr("'techspeed'"))
        .toPandas()
    )

    br_pdf = (
        br_unmatched.select(
            col("record_id").alias("unique_id"),
            "first_name_clean",
            "last_name_clean",
            "state_abbr",
            "election_date",
            "election_year",
            "candidate_office_clean",
            "office_type_clean",
            "email_clean",
            "phone_clean",
            "city_clean",
            "district_clean",
        )
        .withColumn("source_dataset", expr("'ballotready'"))
        .toPandas()
    )

    # Cast election_date to string for Splink compatibility
    for pdf in [ts_pdf, br_pdf]:
        pdf["election_date"] = pdf["election_date"].astype(str)
        # Replace NaN/None with empty string for Splink
        pdf.fillna("", inplace=True)

    # Splink settings
    settings = SettingsCreator(
        link_type="link_only",
        unique_id_column_name="unique_id",
        comparisons=[
            cl.JaroWinklerAtThresholds("first_name_clean", [0.92, 0.85, 0.7]),
            cl.JaroWinklerAtThresholds("last_name_clean", [0.92, 0.85, 0.7]),
            cl.ExactMatch("state_abbr"),
            cl.ExactMatch("election_date").configure(term_frequency_adjustments=True),
            cl.LevenshteinAtThresholds("candidate_office_clean", [1, 3]),
            cl.ExactMatch("office_type_clean"),
            cl.ExactMatch("email_clean"),
            cl.ExactMatch("phone_clean"),
            cl.JaroWinklerAtThresholds("city_clean", [0.92, 0.85]),
            cl.LevenshteinAtThresholds("district_clean", [1, 2]),
        ],
        blocking_rules_to_generate_predictions=[
            block_on("state_abbr", "election_year"),
            block_on("last_name_clean", "state_abbr"),
            block_on("phone_clean"),
        ],
    )

    db_api = DuckDBAPI()
    linker = Linker(
        [ts_pdf, br_pdf],
        settings,
        db_api=db_api,
    )

    # Train model
    linker.training.estimate_u_using_random_sampling(max_pairs=5e6)
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("state_abbr", "last_name_clean"),
        fix_u_probabilities=True,
    )
    linker.training.estimate_parameters_using_expectation_maximisation(
        block_on("email_clean"),
        fix_u_probabilities=True,
    )

    # Predict and filter
    predictions = linker.inference.predict(threshold_match_probability=0.85)
    results_pdf = predictions.as_pandas_dataframe()

    if results_pdf.empty:
        return session.createDataFrame(
            [],
            schema="ts_record_id string, br_record_id string, "
            "splink_match_probability double, splink_match_weight double, "
            "decided_by string",
        )

    # Ensure cross-source matches only (TS <-> BR)
    results_pdf = results_pdf[
        results_pdf["source_dataset_l"] != results_pdf["source_dataset_r"]
    ].copy()

    # Normalize so ts is always on the left
    swap_mask = results_pdf["source_dataset_l"] == "ballotready"
    results_pdf.loc[swap_mask, ["unique_id_l", "unique_id_r"]] = results_pdf.loc[
        swap_mask, ["unique_id_r", "unique_id_l"]
    ].values

    results_pdf = results_pdf.rename(
        columns={
            "unique_id_l": "ts_record_id",
            "unique_id_r": "br_record_id",
            "match_probability": "splink_match_probability",
            "match_weight": "splink_match_weight",
        }
    )
    results_pdf["decided_by"] = "splink"

    # Convert to Spark and enforce 1:1
    result_sdf = session.createDataFrame(
        results_pdf[
            [
                "ts_record_id",
                "br_record_id",
                "splink_match_probability",
                "splink_match_weight",
                "decided_by",
            ]
        ]
    )

    # 1:1 enforcement: keep highest probability per side
    ts_window = Window.partitionBy("ts_record_id").orderBy(
        col("splink_match_probability").desc()
    )
    br_window = Window.partitionBy("br_record_id").orderBy(
        col("splink_match_probability").desc()
    )

    result_sdf = result_sdf.withColumn("ts_rank", expr("row_number()").over(ts_window))
    result_sdf = result_sdf.filter(col("ts_rank") == 1).drop("ts_rank")

    result_sdf = result_sdf.withColumn("br_rank", expr("row_number()").over(br_window))
    result_sdf = result_sdf.filter(col("br_rank") == 1).drop("br_rank")

    return result_sdf.select(
        "ts_record_id",
        "br_record_id",
        "splink_match_probability",
        "splink_match_weight",
        "decided_by",
    )
