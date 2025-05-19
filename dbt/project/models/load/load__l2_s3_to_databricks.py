from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession


def _read_state_data(
    state_id: str,
    s3_base_path: str,
    session: SparkSession,
    s3_access_key: str,
    s3_secret_key: str,
) -> DataFrame:
    """
    This function reads data from S3 for a given state.
    """
    # # Configure Spark session with AWS credentials
    # session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
    # session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
    # session.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # TODO: include both votehistory and demographics
    s3_path = f"{s3_base_path}/{state_id}/VM2--{state_id}--2025-05-10-VOTEHISTORY.tab"
    df = session.read.options(delimiter="\t").csv(
        s3_path, header=True, inferSchema=True
    )
    return df


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model loads data from S3 to Databricks.
    """
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["l2", "s3", "databricks", "load"],
    )

    # s3 configuration
    s3_bucket = dbt.config.get("l2_s3_bucket")
    s3_access_key = dbt.config.get("l2_s3_access_key")
    s3_secret_key = dbt.config.get("l2_s3_secret_key")
    s3_base_path = f"s3://{s3_bucket}/l2_data/from_sftp_server/VMFiles/prod/"

    # TODO: for loop over all states to read and write
    # TODO: use s3 file paths and updated_at's from load__l2_sftp_to_s3
    state_id = "AL".upper()
    state_df = _read_state_data(
        state_id=state_id,
        s3_base_path=s3_base_path,
        session=session,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
    )

    # Ensure the schema exists
    session.sql("CREATE SCHEMA IF NOT EXISTS goodparty_data_catalog.dbt_hugh_source")

    # Now write the table
    state_df.write.mode("overwrite").saveAsTable(
        "goodparty_data_catalog.dbt_hugh_source.l2_al_votehistory"
    )

    load_details = {
        "state_id": state_id,
        "loaded_at": datetime.now(),
    }

    return load_details
