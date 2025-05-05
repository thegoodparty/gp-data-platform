import os
import re

# import tempfile
from datetime import datetime
from typing import Callable, Dict

# import pandas as pd
from paramiko import SFTPClient, Transport
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StringType

# from zipfile import ZipFile


def _extract_and_load_w_sftp(sftp_client: SFTPClient) -> Callable:
    """
    Creates a pandas UDF that extracts and loads files from an SFTP server to an S3 bucket.

    Args:
        sftp_connection: An SFTP client connection

    Returns:
        A pandas UDF function
    """

    def _extract_and_load(state_id: str) -> Dict[str, str | datetime | int]:
        # @pandas_udf(returnType=StringType())
        # def _extract_and_load(state_id: pd.Series) -> pd.Series:
        """
        Pandas UDF to process state IDs in batches and extract and load files from SFTP to S3.

        Args:
            state_id: Series of state IDs

        Returns:
            Series of load details
        """
        """
        TODO: complete writing this function
        List files in the SFTP directory
        create pandas udf that takes in a state id and
        1. filters through the sftp server to find the related files
        2. extracts the zip
        3. loads the files to s3
        4. deletes the old files in s3
        5. returns a an ArrayType with each file name getting it's own row and id,
            it would also have a column for the state id, and time of loading and anything
            else that airbyte logs when using the sftp bulk loader
        """
        # file_name_pattern = f"VM2--{state_id}*.zip"
        file_path = "/VMFiles/"
        file_list = sftp_client.listdir(file_path)
        file_name_pattern = re.compile(r"VM2--[A-Z]{2}--\d{4}-\d{2}-\d{2}\.zip")
        file_list = [f for f in file_list if re.match(file_name_pattern, f)]
        if len(file_list) != 1:
            raise ValueError(
                f"Expected 1 file for state {state_id}, got {len(file_list)}"
            )

        file_name = file_list[0]
        file_path = os.path.join(file_path, file_name)
        # with sftp_client.open(file_path, "rb") as f:
        #     zip_file = ZipFile(f)
        #     zip_file.extractall()
        # return load_details
        return {}

    return _extract_and_load


def model(dbt, session):
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["l2", "sftp", "s3", "load"],
    )

    # connecting to sftp server
    sftp_host = dbt.config.get("l2_sftp_host")
    sftp_port = dbt.config.get("l2_sftp_port")
    sftp_user = dbt.config.get("l2_sftp_user")
    sftp_password = dbt.config.get("l2_sftp_password")

    # Create SFTP connection
    transport: Transport = Transport((sftp_host, int(sftp_port)))
    transport.connect(username=sftp_user, password=sftp_password)
    sftp_client: SFTPClient = SFTPClient.from_transport(transport)

    # get list of states
    states: DataFrame = (
        dbt.ref("stg_airbyte_source__ballotready_s3_uscities_v1_77")
        .select("state_id")
        .distinct()
        .collect()
    )
    states = states.withColumn("state_id", col("state_id").cast(StringType()).upper())

    extract_and_load = _extract_and_load_w_sftp(sftp_client)
    states = states.withColumn("load_details", extract_and_load("state_id"))

    # # Create a temporary directory to store downloaded files
    # with tempfile.TemporaryDirectory() as temp_dir:
    #     # Download files from SFTP to temporary directory
    #     downloaded_files = []
    #     for file_name in file_list:
    #         if file_name.endswith('.csv'):  # Filter for CSV files or adjust as needed
    #             remote_file_path = os.path.join(remote_path, file_name)
    #             local_file_path = os.path.join(temp_dir, file_name)
    #             sftp.get(remote_file_path, local_file_path)
    #             downloaded_files.append(local_file_path)

    #     # Create DataFrame from downloaded files
    #     if downloaded_files:
    #         df = session.read.csv(downloaded_files, header=True, inferSchema=True)
    #     else:
    #         # Create empty DataFrame if no files were downloaded
    #         empty_schema = StructType([StructField("id", StringType(), True)])
    #         df = session.createDataFrame([], empty_schema)

    # # Close SFTP connection
    # sftp.close()
    # transport.close()
