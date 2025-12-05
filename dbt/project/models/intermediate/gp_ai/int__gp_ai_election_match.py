from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException

EMPTY_SCHEMA = StructType(
    [
        StructField("gp_candidacy_id", StringType(), True),
        StructField("ddhq_race_id", IntegerType(), True),
        StructField("ddhq_candidate", StringType(), True),
        StructField("ddhq_candidate_id", IntegerType(), True),
        StructField("ddhq_election_type", StringType(), True),
        StructField("run_id", StringType(), True),
        StructField("has_match", BooleanType(), True),
    ]
)


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model fetches the parquet output file from the int__gp_ai_start_election_match model.
    It loads the parquet file from S3 and returns the election match results.
    """
    # configure the data model
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["gp_candidacy_id", "ddhq_race_id"],
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "gp_ai", "election_match", "fetch"],
    )

    # get the start election match table to extract parquet file paths
    start_election_match_table: DataFrame = dbt.ref("int__gp_ai_start_election_match")

    # prepare incremental loads
    if dbt.is_incremental:
        this_table: DataFrame = session.table(f"{dbt.this}")

    # get only the latest row by created_at
    latest_df = start_election_match_table.orderBy(col("created_at").desc()).limit(1)
    row = latest_df.select(
        "run_id", col("s3_output.files.parquet").alias("parquet_path")
    ).first()

    # if no row found, return empty dataframe
    if not row:
        if dbt.is_incremental:
            return this_table.limit(0)
        else:
            # return empty dataframe - schema will be inferred on first run
            return session.createDataFrame([], EMPTY_SCHEMA)

    run_id: str = row.run_id
    parquet_path: str = row.parquet_path

    # if incremental, check if this run_id already exists
    if dbt.is_incremental:
        existing_run_ids = {
            row.run_id for row in this_table.select("run_id").distinct().collect()
        }
        if run_id in existing_run_ids:
            # already processed, return empty dataframe
            return this_table.limit(0)

    # read parquet file from S3
    # Note: S3 access is configured via Instance Profile
    # see: https://docs.databricks.com/aws/en/connect/storage/tutorial-s3-instance-profile
    try:
        # Disable native Parquet reader to handle INT64 timestamps with nanosecond precision
        # This forces Spark to use its own Parquet reader which can handle these timestamps
        session.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

        parquet_df = session.read.parquet(parquet_path)

        # Convert timestamp_ntz columns to timestamp to avoid Delta timestampNtz feature requirement
        # This prevents the DELTA_FEATURES_REQUIRE_MANUAL_ENABLEMENT error
        for field in parquet_df.schema.fields:
            if isinstance(field.dataType, TimestampNTZType):
                # Convert timestamp_ntz to timestamp (with timezone)
                parquet_df = parquet_df.withColumn(
                    field.name, col(field.name).cast(TimestampType())
                )

        # trigger a cache to ensure the parquet file is loaded
        parquet_df.cache()
        parquet_df.count()
    except AnalysisException:
        # parquet file not found (e.g., PATH_NOT_FOUND error), return empty dataframe
        # This can happen when the election match job hasn't completed yet
        if dbt.is_incremental:
            return this_table.limit(0)
        else:
            # return empty dataframe - schema will be inferred on first run
            return session.createDataFrame([], EMPTY_SCHEMA)

    # add or overwrite run_id column to track which run this data came from
    output_df: DataFrame = parquet_df.withColumn("run_id", lit(run_id).cast("string"))
    output_df = output_df.withColumn("parquet_path", lit(parquet_path).cast("string"))

    # only take the latest instance of each hubspot_gp_candidacy_id and ddhq_race_id combination
    output_df = output_df.orderBy(col("run_id").desc()).dropDuplicates(
        subset=["hubspot_gp_candidacy_id", "ddhq_race_id"]
    )

    # rename columns to remove "hubspot_" prefix
    output_df = output_df.withColumnRenamed("hubspot_row_index", "row_index")
    output_df = output_df.withColumnRenamed(
        "hubspot_gp_candidacy_id", "gp_candidacy_id"
    )
    output_df = output_df.withColumnRenamed("hubspot_candidate_id", "candidate_id")
    output_df = output_df.withColumnRenamed("hubspot_first_name", "first_name")
    output_df = output_df.withColumnRenamed("hubspot_last_name", "last_name")
    output_df = output_df.withColumnRenamed("hubspot_full_name", "full_name")
    output_df = output_df.withColumnRenamed("hubspot_state", "state")
    output_df = output_df.withColumnRenamed("hubspot_city", "city")
    output_df = output_df.withColumnRenamed(
        "hubspot_candidate_office", "candidate_office"
    )
    output_df = output_df.withColumnRenamed(
        "hubspot_official_office_name", "official_office_name"
    )
    output_df = output_df.withColumnRenamed(
        "hubspot_party_affiliation", "party_affiliation"
    )
    output_df = output_df.withColumnRenamed("hubspot_embedding_text", "embedding_text")
    output_df = output_df.withColumnRenamed("hubspot_election_date", "election_date")
    output_df = output_df.withColumnRenamed("hubspot_election_type", "election_type")
    return output_df
