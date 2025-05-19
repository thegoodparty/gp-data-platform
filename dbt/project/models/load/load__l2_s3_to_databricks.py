from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession


def _read_state_data(
    state_id: str,
    s3_base_path: str,
    session: SparkSession,
) -> DataFrame:
    """
    This function reads data from S3 for a given state. Note that read permissions are set by an Instance Profile.
    see: https://docs.databricks.com/aws/en/connect/storage/tutorial-s3-instance-profile#
    see: https://goodparty.atlassian.net/browse/DT-55
    """
    # TODO: include votehistory, demographics, and associated data dictionaries
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
    s3_base_path = f"s3a://{s3_bucket}/l2_data/from_sftp_server/VMFiles/prod/"

    s3_files_loaded: DataFrame = dbt.ref("load__l2_sftp_to_s3")
    state_list = [
        row.state_id for row in s3_files_loaded.select("state_id").distinct().collect()
    ]
    # TODO: for loop over all states (or maybe just files) to read and write
    # TODO: use s3 file paths and updated_at's from load__l2_sftp_to_s3
    # TODO: incremental strategy based on updated_at for the state and file name of the s3
    for index, state_id in enumerate(state_list):
        if index == 0:
            # Ensure the schema exists
            session.sql(
                "CREATE SCHEMA IF NOT EXISTS goodparty_data_catalog.dbt_hugh_source"
            )
        if state_id == "AL":
            # set up reader
            state_df = _read_state_data(
                state_id=state_id,
                s3_base_path=s3_base_path,
                session=session,
            )

            # Now write the table
            # TODO: wrap this in a function and return load details
            state_df.write.mode("overwrite").saveAsTable(
                f"goodparty_data_catalog.dbt_hugh_source.l2_s3_{state_id}_votehistory"
            )

    load_details = {
        "state_id": state_id,
        "loaded_at": datetime.now(),
        # "source_file_name": ,
        # "table_name": ,
    }

    # TODO: convert load details to a table
    return load_details
