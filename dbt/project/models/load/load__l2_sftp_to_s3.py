import logging
import os
import re
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict
from uuid import uuid4
from zipfile import ZipFile

import boto3
import pandas as pd
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


def _extract_and_load_w_creds(
    sftp_host: str,
    sftp_port: int,
    sftp_user: str,
    sftp_password: str,
    s3_bucket: str,
    s3_prefix: str,
    s3_access_key: str,
    s3_secret_key: str,
) -> Callable:
    """
    Creates a pandas UDF that extracts and loads files from an SFTP server to an S3 bucket. Note that
    the SFTPClient cannot be serilized to be passed to the UDF workers. The connection must be established
    on the worker nodes.

    Args:
        sftp_host: The host of the SFTP server
        sftp_port: The port of the SFTP server
        sftp_user: The user of the SFTP server
        sftp_password: The password of the SFTP server
        s3_bucket: The name of the S3 bucket
        s3_access_key: The access key for the S3 bucket
        s3_secret_key: The secret key for the S3 bucket

    Returns:
        A pandas UDF function
    """

    def _extract_and_load(state_id: str) -> Dict[str, Any]:
        """
        UDF to process state IDs and extract and load files from SFTP to S3.

        Args:
            state_id: Series of state IDs

        Returns:
            JSON string with load details
        """
        try:
            # Create SFTP connection inside the UDF
            transport: Transport = Transport((sftp_host, sftp_port))
            transport.connect(username=sftp_user, password=sftp_password)
            sftp_client: SFTPClient | None = SFTPClient.from_transport(transport)
            if sftp_client is None:
                raise ValueError("Failed to create SFTP client")

            # create s3 client
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
            )

            # collect existing files in the s3 bucket
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

            # check if the base file name is the same as the file name in the s3 bucket
            source_file_name = file_list[0]
            source_file_base_name = source_file_name.split(".")[0]
            s3_state_prefix = f"{s3_prefix}/{state_id.upper()}/"
            s3_file_list = s3_client.list_objects_v2(
                Bucket=s3_bucket, Prefix=s3_state_prefix
            )
            s3_file_list = [f["Key"] for f in s3_file_list.get("Contents", [])]
            s3_file_exists = any(
                source_file_base_name in s3_file_name for s3_file_name in s3_file_list
            )
            if s3_file_exists:
                logging.info(
                    f"File with base name {source_file_base_name} already exists in S3"
                )
                return {
                    "state_id": None,
                    "source_file_names": None,
                    "source_zip_file": None,
                    "loaded_at": None,
                }

            # download the file from the sftp server and extract it
            full_file_path = os.path.join(remote_file_path, source_file_name)

            # Create a temporary directory to store the zip file and extracted contents
            with TemporaryDirectory() as temp_dir:
                # Download the file from the SFTP server
                local_zip_path = os.path.join(temp_dir, source_file_name)
                sftp_client.get(full_file_path, local_zip_path)

                """
                Files inside of the zip are named like:
                'VM2--{state_id}--{YYYY-MM-DD}-DEMOGRAPHIC-FillRate.tab'
                'VM2--{state_id}--{YYYY-MM-DD}-DEMOGRAPHIC.tab'
                'VM2--{state_id}--{YYYY-MM-DD}-DEMOGRAPHIC_DataDictionary.csv'
                'VM2--{state_id}--{YYYY-MM-DD}-VOTEHISTORY.tab'
                'VM2--{state_id}--{YYYY-MM-DD}-VOTEHISTORY_DataDictionary.csv'
                """
                with ZipFile(local_zip_path, "r") as zip_file:
                    file_names = zip_file.namelist()
                    zip_file.extractall(path=temp_dir)

                # delete the zip file
                os.remove(local_zip_path)

                # Process files one at a time to manage memory
                file_name_paths = [
                    os.path.join(temp_dir, file_name) for file_name in file_names
                ]
                file_name_paths_to_upload = []
                pattern = r"^VM2--[A-Z]{2}--\d{4}-\d{2}-\d{2}-(DEMOGRAPHIC|VOTEHISTORY)(_DataDictionary\.csv|\.tab)$"

                for file in file_name_paths:
                    filename = os.path.basename(file)
                    # Skip FillRate files
                    if "FillRate" in filename:
                        logging.info(f"Skipping FillRate file: {filename}")
                        continue

                    match = re.match(pattern, filename)
                    if match:
                        file_name_paths_to_upload.append(file)

                        # Extract the file type (DEMOGRAPHIC or VOTEHISTORY/VOTERHISTORY) and extension
                        file_type = match.group(1)
                        extension = match.group(2)  # _DataDictionary.csv or .tab

                        # headers and footers must be removed from data dictionary files
                        if (
                            file_type == "DEMOGRAPHIC"
                            and extension == "_DataDictionary.csv"
                        ):
                            df = pd.read_csv(file, skiprows=15, skipfooter=24)
                            df.to_csv(file, index=False)
                        elif (
                            file_type == "VOTEHISTORY"
                            and extension == "_DataDictionary.csv"
                        ):
                            df = pd.read_csv(file, skiprows=15, skipfooter=4)
                            df.to_csv(file, index=False)
                    else:
                        logging.info(f"Skipping (match={match}) file: {filename}")

                # Upload each extracted file to S3
                for extracted_file in file_names:
                    local_file_path = os.path.join(temp_dir, extracted_file)
                    s3_key = f"{s3_state_prefix}{extracted_file}"

                    if os.path.isfile(local_file_path):
                        s3_client.upload_file(
                            Filename=local_file_path, Bucket=s3_bucket, Key=s3_key
                        )

                # Delete files from s3 prefix that are not in the zip file
                valid_files = [f for f in file_names if re.match(pattern, f)]
                for s3_file_name in s3_file_list:
                    if s3_file_name not in valid_files:
                        s3_client.delete_object(Bucket=s3_bucket, Key=s3_file_name)

                # delete locally extracted files
                for file in file_name_paths_to_upload:
                    os.remove(file)

        finally:
            # Close SFTP connection
            if "sftp_client" in locals():
                if sftp_client is not None:
                    sftp_client.close()
            if "transport" in locals():
                transport.close()

        result = {
            "state_id": state_id,
            "source_file_names": file_names,
            "source_zip_file": source_file_name,
            "loaded_at": datetime.now(),
            "s3_state_prefix": s3_state_prefix,
        }
        return result

    # Define the schema for the dictionary
    return_schema = StructType(
        [
            StructField(name="state_id", dataType=StringType(), nullable=True),
            StructField(
                name="source_file_names",
                dataType=ArrayType(StringType()),
                nullable=True,
            ),
            StructField(name="source_zip_file", dataType=StringType(), nullable=True),
            StructField(name="loaded_at", dataType=TimestampType(), nullable=True),
            StructField(name="s3_state_prefix", dataType=StringType(), nullable=True),
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

    # sftp server configuration
    sftp_host = dbt.config.get("l2_sftp_host")
    sftp_port = int(dbt.config.get("l2_sftp_port"))
    sftp_user = dbt.config.get("l2_sftp_user")
    sftp_password = dbt.config.get("l2_sftp_password")

    # S3 configuration
    s3_bucket = dbt.config.get("l2_s3_bucket")
    s3_access_key = dbt.config.get("l2_s3_access_key")
    s3_secret_key = dbt.config.get("l2_s3_secret_key")
    l2_vmfiles_prefix = "l2_data/from_sftp_server/VMFiles"

    # get list of states
    states: DataFrame = (
        dbt.ref("stg_airbyte_source__ballotready_s3_uscities_v1_77")
        .select("state_id")
        .distinct()
    )
    states = states.withColumn("state_id", upper(col("state_id").cast(StringType())))

    # trigger preceding transformations and filter out nulls
    states.cache()
    states.count()
    # for dev just a few states
    states = states.filter(col("state_id").isin(["AK", "DE", "RI"]))
    # states = states.limit(5)
    # TODO: remove dev restiction above

    # Repartition to ensure one state per partition
    states = states.repartition(states.count())

    # extract and load the files
    extract_and_load = _extract_and_load_w_creds(
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_user=sftp_user,
        sftp_password=sftp_password,
        s3_bucket=s3_bucket,
        s3_prefix=l2_vmfiles_prefix,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
    )
    states = states.withColumn("load_details", extract_and_load(states["state_id"]))

    # generate an uuid for the load job
    states = states.withColumn("load_id", lit(str(uuid4())))

    # Explode the file_names array to create separate rows for each filename
    exploded_states = states.select(
        col("load_id"),
        col("load_details.loaded_at").alias("loaded_at"),
        explode(col("load_details.source_file_names")).alias("source_file_name"),
        col("load_details.source_zip_file").alias("source_zip_file"),
        col("load_details.state_id").alias("state_id"),
        col("load_details.s3_state_prefix").alias("s3_state_prefix"),
    )

    # generate an uuid for each file name
    generate_uuid = udf(f=lambda: str(uuid4()), returnType=StringType())
    exploded_states = exploded_states.withColumn("id", generate_uuid())

    exploded_states = exploded_states.select(
        "id",
        "load_id",
        "loaded_at",
        "state_id",
        "source_file_name",
        "source_zip_file",
        "s3_state_prefix",
    )

    # trigger preceding transformations and filter out nulls
    exploded_states.cache()
    exploded_states.count()
    exploded_states = exploded_states.filter(col("state_id").isNotNull())
    exploded_states = exploded_states.filter(col("source_file_name").isNotNull())
    exploded_states = exploded_states.filter(col("source_zip_file").isNotNull())

    return exploded_states
