from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

EMPTY_SCHEMA = StructType(
    [
        StructField("ddhq_race_id", StringType(), True),
        StructField("ddhq_candidate_id", StringType(), True),
        StructField("run_id", StringType(), True),
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
        incremental_strategy="append",
        unique_key=[
            "ddhq_race_id",
            "ddhq_candidate_id",
        ],  # probably will need to change this
        on_schema_change="append_new_columns",
        auto_liquid_cluster=True,
        tags=["intermediate", "gp_ai", "election_match", "fetch"],
    )

    # get the start election match table to extract parquet file paths
    start_election_match_table: DataFrame = dbt.ref("int__gp_ai_start_election_match")

    # get only the latest row by created_at
    window_spec = Window.orderBy(col("created_at").desc())
    start_election_match_table = (
        start_election_match_table.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # collect the single parquet file path to process
    row = start_election_match_table.select(
        "run_id", col("s3_output.files.parquet").alias("parquet_path")
    ).first()

    # if no row found, return empty dataframe
    if not row:
        if dbt.is_incremental:
            this_table: DataFrame = session.table(f"{dbt.this}")
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

        # trigger a cache to ensure the parquet file is loaded
        parquet_df.count()
        parquet_df.cache()
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

    return output_df
