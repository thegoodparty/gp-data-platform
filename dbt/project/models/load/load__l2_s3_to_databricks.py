from datetime import datetime
from uuid import uuid4

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, row_number
from pyspark.sql.session import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


def _filter_latest_loaded_files(df: DataFrame) -> DataFrame:
    """
    Filter the dataframe to only include latest loaded files that match the file patterns
    """
    df = df.filter(
        (col("source_file_name").like("%VOTEHISTORY.tab"))
        | (col("source_file_name").like("%DEMOGRAPHIC.tab"))
        | (col("source_file_name").like("%VOTEHISTORY_DataDictionary.csv"))
        | (col("source_file_name").like("%DEMOGRAPHIC_DataDictionary.csv"))
        | (col("source_file_name").like("VM2Uniform%.tab"))
        | (col("source_file_name").like("VM2Uniform%DataDictionary.csv"))
    )

    # define window to partition by source_dile_name and order by loaded_at descending
    window_spec = Window.partitionBy("source_file_name").orderBy(
        col("loaded_at").desc()
    )

    # add row number and select only most recent record (row_number = 1)
    df = (
        df.withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    return df


def _extract_table_name(source_file_name: str, state_id: str) -> str:
    """
    Extract the table name from the source file name.
    # TODO: handle uniform files

    Examples:
        >>> _extract_table_name('VM2--AL--2025-05-10-VOTEHISTORY.tab', 'AL')
        'l2_s3_al_vote_history'
        >>> _extract_table_name('VM2--NY--2025-05-10-DEMOGRAPHIC.tab', 'NY')
        'l2_s3_ny_demographic'
        >>> _extract_table_name('VM2--CA--2025-05-10-VOTEHISTORY_DataDictionary.csv', 'CA')
        'l2_s3_ca_vote_history_data_dictionary'
        >>> _extract_table_name('VM2--TX--2025-05-10-DEMOGRAPHIC_DataDictionary.csv', 'TX')
        'l2_s3_tx_demographic_data_dictionary'
        >>> _extract_table_name('VM2Uniform--AK--2025-05-10.tab', 'AK')
        'l2_s3_ak_uniform_data_dictionary'
        >>> _extract_table_name('VM2Uniform--AK--2025-05-10_DataDictionary.csv', 'AK')
        'l2_s3_ak_uniform_data_dictionary'
    """
    # Extract the file type from the source file name
    file_type = source_file_name.split("--")[-1].split(".")[0].lower()

    # Remove the date from the file type. Uniform files are handled separately.
    if "uniform" in source_file_name.lower():
        if "datadictionary" in source_file_name.lower():
            file_type = "uniform_data_dictionary"
        else:
            file_type = "uniform"
    else:
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

    # get dbt environment variables
    dbt_env_name = dbt.config.get("dbt_environment")
    s3_bucket = dbt.config.get("l2_s3_bucket")

    # set databricks schema based on dbt cloud environment name
    # TODO: use schema based on dbt cloud account. current env vars listed in docs are not available
    # see https://docs.getdbt.com/docs/build/environment-variables#special-environment-variables
    if dbt_env_name == "dev":
        databricks_schema = "dbt_hugh_source"
    elif dbt_env_name == "prod":
        databricks_schema = "dbt_source"
    else:
        raise ValueError(f"Invalid `dbt_env_name`: {dbt_env_name}")

    # get files loaded from sftp server into s3
    s3_files_loaded: DataFrame = dbt.ref("load__l2_sftp_to_s3")
    state_list = [
        row.state_id for row in s3_files_loaded.select("state_id").distinct().collect()
    ]

    # initialize list to capture metadata about data loads
    load_details = []

    # Ensure the schema exists
    session.sql(
        f"CREATE SCHEMA IF NOT EXISTS goodparty_data_catalog.{databricks_schema}"
    )
    for state_id in state_list:
        state_files_loaded = s3_files_loaded.filter(col("state_id") == state_id)

        # get the latest loaded_at for the state
        latest_files = _filter_latest_loaded_files(state_files_loaded)

        # if incremental, filter the latest_files to only include files yet to be loaded
        if dbt.is_incremental:
            # TODO: the incremental logic needs to be applied to each row/file in latest_files
            this_table = session.table(f"{dbt.this}")
            this_table = this_table.filter(col("state_id") == state_id)
            last_load_this_table = this_table.agg({"loaded_at": "max"}).collect()[0][0]
            if last_load_this_table:
                latest_files = latest_files.filter(
                    col("loaded_at") > last_load_this_table
                )

        # if there are no files to load, skip the state and move on to the next
        if latest_files.count() == 0:
            continue

        for file in latest_files.toLocalIterator():
            source_file_name = file.source_file_name
            table_name = _extract_table_name(source_file_name, state_id)

            # set up reader and add loaded_at column
            delimiter = "\t" if source_file_name.endswith(".tab") else ","
            s3_path = f"s3a://{s3_bucket}/{file.s3_state_prefix}/{source_file_name}"
            data_df = session.read.options(delimiter=delimiter).csv(
                path=s3_path,
                header=True,
                inferSchema=True,
            )
            data_df = data_df.withColumn("loaded_at", current_timestamp())

            # TODO: set table path based on dbt environment and compare against airbyte naming
            table_path = f"goodparty_data_catalog.{databricks_schema}.{table_name}"
            data_df.write.mode("overwrite").option("overwriteSchema", "true").option(
                "clusterByAuto", "true"
            ).format("delta").saveAsTable(table_path)
            load_details.append(
                {
                    "id": str(uuid4()),
                    "state_id": state_id,
                    "source_s3_path": s3_path,
                    "source_file_name": source_file_name,
                    "table_name": table_name,
                    "table_path": table_path,
                }
            )

    # log load details to table
    load_id = str(uuid4())
    for load_data in load_details:
        load_data["load_id"] = load_id
        load_data["loaded_at"] = datetime.now()

    load_details_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("load_id", StringType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("state_id", StringType(), True),
            StructField("source_s3_path", StringType(), True),
            StructField("source_file_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("table_path", StringType(), True),
        ]
    )
    load_details_df = session.createDataFrame(load_details, load_details_schema)
    return load_details_df
