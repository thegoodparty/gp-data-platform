from datetime import datetime
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession


def _extract_table_name(source_file_name: str, state_id: str) -> str:
    """
    Extract the table name from the source file name.

    Examples:
        >>> _extract_table_name('VM2--AL--2025-05-10-VOTEHISTORY.tab', 'AL')
        'l2_s3_al_vote_history'
        >>> _extract_table_name('VM2--NY--2025-05-10-DEMOGRAPHIC.tab', 'NY')
        'l2_s3_ny_demographic'
        >>> _extract_table_name('VM2--CA--2025-05-10-VOTEHISTORY_DataDictionary.csv', 'CA')
        'l2_s3_ca_vote_history_data_dictionary'
        >>> _extract_table_name('VM2--TX--2025-05-10-DEMOGRAPHIC_DataDictionary.csv', 'TX')
        'l2_s3_tx_demographic_data_dictionary'
    """
    # Extract the file type from the source file name
    file_type = source_file_name.split("--")[-1].split(".")[0].lower()

    # Remove the date from the file type
    file_type = file_type.split("-")[-1]

    # Construct the table name based on file type and whether it's a data dictionary
    table_name = f"l2_s3_{state_id.lower()}_{file_type}".replace(
        "datadictionary", "data_dictionary"
    )
    table_name = table_name.replace("votehistory", "vote_history")

    return table_name


def model(dbt, session: SparkSession) -> DataFrame:
    """
    This model loads data from S3 to Databricks.
    Note that read permissions are set by an Instance Profile.
    see: https://docs.databricks.com/aws/en/connect/storage/tutorial-s3-instance-profile#
    see: https://goodparty.atlassian.net/browse/DT-55
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

    s3_files_loaded: DataFrame = dbt.ref("load__l2_sftp_to_s3")
    state_list = [
        row.state_id for row in s3_files_loaded.select("state_id").distinct().collect()
    ]

    # TODO: remove this dev filter
    state_list = [state for state in state_list if state in ["AL"]]

    # TODO: use s3 file paths and updated_at's from load__l2_sftp_to_s3
    # TODO: incremental strategy based on updated_at for the state and file name of the s3
    # Ensure the schema exists
    session.sql("CREATE SCHEMA IF NOT EXISTS goodparty_data_catalog.dbt_hugh_source")
    for state_id in state_list:

        # get the latest loaded_at for the state
        latest_date = (
            s3_files_loaded.filter(col("state_id") == state_id)
            .orderBy(col("loaded_at").desc())
            .first()
            .loaded_at
        )

        # take the the latest files having a distinct s3_state_prefix and with the latest loaded_at
        latest_files = s3_files_loaded.filter(
            col("state_id") == state_id,
            col("loaded_at") == latest_date,
        )

        # TODO: if incremental, filter the latest_files to only include files with a later updated_at
        load_details = []
        for file in latest_files:
            source_file_name = file.source_file_name
            table_name = _extract_table_name(source_file_name, state_id)

            # set up reader
            delimiter = "\t" if source_file_name.endswith(".tab") else ","
            s3_path = f"s3a://{s3_bucket}/{file.s3_state_prefix}/{source_file_name}"
            data_df = session.read.options(delimiter=delimiter).csv(
                path=s3_path,
                header=True,
                inferSchema=True,
            )

            # Write the table
            # TODO: wrap this in a function and return load details (or maybe just script the details here)
            data_df.write.mode("overwrite").saveAsTable(
                f"goodparty_data_catalog.dbt_hugh_source.{table_name}"
            )
            load_details.append(
                {
                    "id": str(uuid4()),
                    "loaded_at": datetime.now(),
                    "state_id": state_id,
                    "source_s3_path": s3_path,
                    "source_file_name": source_file_name,
                    "table_name": table_name,
                }
            )

    # TODO: convert load details to a table
    return load_details
