import os
import re
from datetime import datetime

# import pandas as pd
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict
from uuid import uuid4
from zipfile import ZipFile

from paramiko import SFTPClient, Transport
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, lit, udf, upper
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# import boto3


# from zipfile import ZipFile


def _extract_and_load_w_sftp(
    host: str, port: int, user: str, password: str
) -> Callable:
    """
    Creates a pandas UDF that extracts and loads files from an SFTP server to an S3 bucket. Note that
    the SFTPClient cannot be serilized to be passed to the UDF workers. The connection must be established
    on the worker nodes.

    Args:
        host: The host of the SFTP server
        port: The port of the SFTP server
        user: The user of the SFTP server
        password: The password of the SFTP server

    Returns:
        A pandas UDF function
    """

    def _extract_and_load(state_id: str) -> Dict[str, Any]:
        # def _extract_and_load(state_id: str) -> Dict[str, Union[str, List[str], datetime]]:
        """
        UDF to process state IDs and extract and load files from SFTP to S3.

        Args:
            state_id: Series of state IDs

        Returns:
            JSON string with load details
        """
        # Create SFTP connection inside the UDF
        transport: Transport = Transport((host, port))
        transport.connect(username=user, password=password)
        sftp_client: SFTPClient | None = SFTPClient.from_transport(transport)
        if sftp_client is None:
            raise ValueError("Failed to create SFTP client")

        try:
            remote_file_path = "/VMFiles/"
            file_list = sftp_client.listdir(remote_file_path)
            string_pattern = f"VM2--{state_id}" + r"--\d{4}-\d{2}-\d{2}\.zip"
            file_name_pattern = re.compile(string_pattern)
            file_list = [f for f in file_list if re.match(file_name_pattern, f)]
            if len(file_list) != 1:
                raise ValueError(
                    f"Expected 1 file for state {state_id}, got {len(file_list)}\n"
                    f"string_pattern: {string_pattern}\n"
                    f"file_name_pattern: {file_name_pattern}\n"
                    f"file_list: {file_list}\n"
                )
            # download the file from the sftp server and extract it
            source_file_name = file_list[0]
            full_file_path = os.path.join(remote_file_path, source_file_name)

            # TODO: extract the zip files
            # Create a temporary directory to store the zip file and extracted contents
            with TemporaryDirectory() as temp_dir:
                # Download the file from the SFTP server
                local_zip_path = os.path.join(temp_dir, source_file_name)
                sftp_client.get(full_file_path, local_zip_path)

                # Extract the zip file
                # file_names = []
                with ZipFile(local_zip_path, "r") as zip_file:
                    file_names = zip_file.namelist()
                    zip_file.extractall(path=temp_dir)

        # TODO: process files, like removing headers and footers

        # TODO: upload files to s3
        # # Upload extracted files to S3
        # s3_client = boto3.client('s3')
        # s3_prefix = f"states/{state_id}/data/"

        # # Upload each extracted file to S3
        # for extracted_file in file_names:
        #     local_file_path = os.path.join(temp_dir, extracted_file)
        #     s3_key = f"{s3_prefix}{extracted_file}"

        #     if os.path.isfile(local_file_path):
        #         s3_client.upload_file(
        #             Filename=local_file_path,
        #             Bucket=s3_bucket_name,
        #             Key=s3_key
        #         )

        # TODO: delete old files from s3 prefix

        finally:
            # Close SFTP connection
            sftp_client.close()
            transport.close()

        result = {
            "state_id": state_id,
            # "source_file_names": [source_file_name],
            "source_file_names": file_names,  # use extracted file names
            "source_zip_file": source_file_name,
            "loaded_at": datetime.now(),
            # "s3_prefix": s3_prefix
        }
        return result

    # Define the schema for the dictionary
    return_schema = StructType(
        [
            StructField(name="state_id", dataType=StringType(), nullable=True),
            StructField(
                name="source_file_names",
                dataType=ArrayType(StringType()),
                nullable=False,
            ),
            StructField(name="source_zip_file", dataType=StringType(), nullable=False),
            StructField(name="loaded_at", dataType=TimestampType(), nullable=False),
        ]
    )
    _extract_and_load = udf(_extract_and_load, returnType=return_schema)
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

    # S3 configuration
    # s3_bucket = dbt.var("s3_bucket", "data-platform-prod")

    # connecting to sftp server
    sftp_host = dbt.config.get("l2_sftp_host")
    sftp_port = int(dbt.config.get("l2_sftp_port"))
    sftp_user = dbt.config.get("l2_sftp_user")
    sftp_password = dbt.config.get("l2_sftp_password")

    # get list of states
    states: DataFrame = (
        dbt.ref("stg_airbyte_source__ballotready_s3_uscities_v1_77")
        .select("state_id")
        .distinct()
    )
    states = states.withColumn("state_id", upper(col("state_id").cast(StringType())))

    # for dev just take AK
    states = states.filter(col("state_id") == "AK")
    # TODO: remove dev restiction above

    # force evaluation of the states dataframe up until now
    states.cache()
    states.count()

    # extract and load the files
    extract_and_load = _extract_and_load_w_sftp(
        sftp_host, sftp_port, sftp_user, sftp_password
    )
    states = states.withColumn("load_details", extract_and_load(states["state_id"]))

    # generate an uuid for each file name
    states = states.withColumn("load_id", lit(str(uuid4())))

    # Explode the file_names array to create separate rows for each filename
    exploded_states = states.select(
        col("load_id"),
        col("load_details.loaded_at").alias("loaded_at"),
        explode(col("load_details.source_file_names")).alias("source_file_name"),
        col("load_details.source_zip_file").alias("source_zip_file"),
        col("load_details.state_id").alias("state_id"),
    )

    # generate an uuid for each file name
    generate_uuid = udf(lambda: str(uuid4()), StringType())
    exploded_states = exploded_states.withColumn("id", generate_uuid())

    exploded_states = exploded_states.select(
        "id",
        "load_id",
        "loaded_at",
        "state_id",
        "source_file_name",
        "source_zip_file",
    )
    return exploded_states
