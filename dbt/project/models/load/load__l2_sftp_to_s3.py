import logging
import os
import re
import shutil
import time
import traceback
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict
from uuid import uuid4
from zipfile import ZipFile

import boto3
import pandas as pd
from paramiko import SFTPClient, Transport
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, explode, lit, udf, upper
from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EMPTY_LOAD_DETAILS = {
    "state_id": None,
    "source_file_names": None,
    "source_zip_file": None,
    "loaded_at": None,
    "s3_state_prefix": None,
}


def _create_sftp_connection(
    host: str,
    port: int,
    username: str,
    password: str,
    max_retries: int = 3,
    retry_delay: int = 5,
) -> tuple[Transport, SFTPClient]:
    """
    Creates an SFTP connection with retry logic and keep-alive settings.

    Args:
        host: SFTP host
        port: SFTP port
        username: SFTP username
        password: SFTP password
        max_retries: Maximum number of connection attempts
        retry_delay: Delay between retries in seconds

    Returns:
        Tuple of (Transport, SFTPClient)
    """
    for attempt in range(max_retries):
        try:
            transport = Transport((host, port))
            transport.set_keepalive(30)
            transport.connect(username=username, password=password)
            sftp_client = SFTPClient.from_transport(transport)
            if sftp_client is None:
                raise ValueError("Failed to create SFTP client")
            logging.info("SFTP client created successfully")
            return transport, sftp_client
        except Exception as e:
            logging.error(f"SFTP connection attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise
            logging.warning(f"Waiting {retry_delay} seconds before next attempt...")
            time.sleep(retry_delay)
    raise Exception("Failed to establish SFTP connection after all retries")


def _extract_and_load_w_params(
    sftp_host: str,
    sftp_port: int,
    sftp_user: str,
    sftp_password: str,
    s3_bucket: str,
    s3_prefix: str,
    s3_access_key: str,
    s3_secret_key: str,
    databricks_volume_directory: str,
    is_uniform: bool,
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
        databricks_volume_directory: The directory to store temporary files
        is_uniform: Whether the files are uniform or not

    Returns:
        A pandas UDF function
    """

    def _extract_and_load(state_id: str) -> Dict[str, Any]:
        """
        UDF to process state IDs and extract and load files from SFTP to S3.

        Args:
            state_id: Series of state IDs

        Returns:
            Dictionary with details of loading job
        """
        logging.info(f"Processing state: {state_id}")
        transport = None
        sftp_client = None
        try:
            # Create SFTP connection inside the UDF with retry logic
            transport, sftp_client = _create_sftp_connection(
                host=sftp_host,
                port=sftp_port,
                username=sftp_user,
                password=sftp_password,
            )

            # create s3 client
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
            )

            # get file for the state from the sftp server and check that it exists
            if is_uniform:
                remote_file_path = "/VM2Uniform"
                string_pattern = f"VM2Uniform--{state_id}" + r"--\d{4}-\d{2}-\d{2}\.zip"
            else:
                remote_file_path = "/VMFiles/"
                string_pattern = f"VM2--{state_id}" + r"--\d{4}-\d{2}-\d{2}\.zip"

            file_list = sftp_client.listdir(remote_file_path)
            file_name_pattern = re.compile(string_pattern)
            file_list = [f for f in file_list if re.match(file_name_pattern, f)]
            if len(file_list) != 1:
                raise ValueError(
                    f"Expected 1 file for state {state_id}, got {len(file_list)}\n"
                    f"string_pattern: {string_pattern}\n"
                    f"file_name_pattern: {file_name_pattern}\n"
                    f"file_list: {file_list}\n"
                )

            # check if the base file name is the same as a file name in the s3 bucket
            # if it is, skip the load since the date in the file name is the same. no updates available.
            source_zip_file_name = file_list[0]
            source_zip_file_base_name = source_zip_file_name.split(".")[0]
            s3_state_prefix = f"{s3_prefix}/{state_id.upper()}/"
            s3_file_list = s3_client.list_objects_v2(
                Bucket=s3_bucket, Prefix=s3_state_prefix
            )
            s3_file_list = [f["Key"] for f in s3_file_list.get("Contents", [])]
            s3_file_exists = any(
                source_zip_file_base_name in s3_file_name
                for s3_file_name in s3_file_list
            )
            if s3_file_exists:
                logging.info(
                    f"File with base name {source_zip_file_base_name} already exists in S3"
                )
                return EMPTY_LOAD_DETAILS

            # download the file from the sftp server and extract it
            full_file_path = os.path.join(remote_file_path, source_zip_file_name)

            with TemporaryDirectory(
                prefix=f"temp_{state_id}_", dir=databricks_volume_directory
            ) as temp_extract_dir:
                # Download the file from the SFTP server
                local_zip_path = os.path.join(temp_extract_dir, source_zip_file_name)

                try:
                    sftp_client.get(
                        remotepath=full_file_path,
                        localpath=local_zip_path,
                        max_concurrent_prefetch_requests=64,
                    )
                except OSError as e:
                    logging.error(
                        f"Source file from sftp server {full_file_path} is locked: {str(e)}."
                        " This may happen when source SFTP server is being updated."
                        " Skipping for now. Will retry later."
                    )
                    return EMPTY_LOAD_DETAILS

                """
                Files inside of the zip are named like:
                'VM2--{state_id}--{YYYY-MM-DD}-DEMOGRAPHIC-FillRate.tab'
                'VM2--{state_id}--{YYYY-MM-DD}-DEMOGRAPHIC.tab'
                'VM2--{state_id}--{YYYY-MM-DD}-DEMOGRAPHIC_DataDictionary.csv'
                'VM2--{state_id}--{YYYY-MM-DD}-VOTEHISTORY.tab'
                'VM2--{state_id}--{YYYY-MM-DD}-VOTEHISTORY_DataDictionary.csv'

                Or for the uniform files:
                'VM2Uniform--{state_id}--{YYYY-MM-DD}.tab'
                'VM2Uniform--{state_id}--{YYYY-MM-DD}_DataDictionary.csv'
                """
                try:
                    with ZipFile(local_zip_path, "r") as zip_file:
                        extracted_file_names = zip_file.namelist()
                        zip_file.extractall(path=temp_extract_dir)
                except NotImplementedError:
                    logging.error(
                        f"Source zip file {local_zip_path} is corrupted."
                        " This may happen when source SFTP server is being updated."
                        " Skipping for now. Will retry later."
                    )
                    return EMPTY_LOAD_DETAILS

                # delete the zip file
                os.remove(local_zip_path)

                # get list of files to upload
                file_name_paths = [
                    os.path.join(temp_extract_dir, file_name)
                    for file_name in extracted_file_names
                ]
                file_names_to_upload = []
                if is_uniform:
                    pattern = r"^VM2Uniform--[A-Z]{2}--\d{4}-\d{2}-\d{2}(_DataDictionary\.csv|\.tab)$"
                else:
                    pattern = r"^VM2--[A-Z]{2}--\d{4}-\d{2}-\d{2}-(DEMOGRAPHIC|VOTEHISTORY)(_DataDictionary\.csv|\.tab)$"

                for file_path in file_name_paths:
                    filename = os.path.basename(file_path)
                    # Skip FillRate files
                    if "FillRate" in filename:
                        logging.info(f"Skipping FillRate file: {filename}")
                        continue

                    match = re.match(pattern, filename)
                    if match:
                        file_names_to_upload.append(filename)

                        # Extract the file type (DEMOGRAPHIC or VOTEHISTORY/VOTERHISTORY) and extension
                        if is_uniform:
                            file_type = "UNIFORM"
                            extension = match.group(1)  # _DataDictionary.csv or .tab
                        else:
                            file_type = match.group(1)
                            extension = match.group(2)  # _DataDictionary.csv or .tab

                        # headers and footers must be removed from data dictionary files
                        if (
                            file_type in ["DEMOGRAPHIC", "UNIFORM"]
                            and extension == "_DataDictionary.csv"
                        ):
                            df = pd.read_csv(file_path, skiprows=15, skipfooter=24)
                            df.to_csv(file_path, index=False)
                        elif (
                            file_type == "VOTEHISTORY"
                            and extension == "_DataDictionary.csv"
                        ):
                            df = pd.read_csv(file_path, skiprows=15, skipfooter=4)
                            df.to_csv(file_path, index=False)
                    else:
                        logging.info(f"Skipping (match={match}) file: {filename}")

                    local_file_path = os.path.join(temp_extract_dir, filename)
                    s3_key = f"{s3_state_prefix}{filename}"

                    if os.path.isfile(local_file_path):
                        s3_client.upload_file(
                            Filename=local_file_path, Bucket=s3_bucket, Key=s3_key
                        )
                        # delete locally extracted files
                        os.remove(local_file_path)

                        # delete old versions of file from s3, same prefix and suffix but different date
                        for existing_s3_file in s3_file_list:
                            s3_regexp_match = re.match(pattern, existing_s3_file)
                            if s3_regexp_match and existing_s3_file != filename:
                                s3_client.delete_object(
                                    Bucket=s3_bucket, Key=existing_s3_file
                                )

        except Exception as e:
            logging.error(f"Error processing state {state_id}: {str(e)}")
            error_details = traceback.format_exc()
            logging.error(f"Full exception details:\n{error_details}")
            raise Exception(
                f"Error processing state {state_id}: {str(e)}\nFull traceback:\n{error_details}"
            )
        finally:
            # Close SFTP connection
            if sftp_client is not None:
                sftp_client.close()
            if transport is not None:
                transport.close()

        result = {
            "state_id": state_id,
            "source_file_names": file_names_to_upload,
            "source_zip_file": source_zip_file_name,
            "loaded_at": datetime.now(),
            "s3_state_prefix": s3_state_prefix,
        }
        return result

    return _extract_and_load


def model(dbt, session: SparkSession):
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
    dbt_env_name = dbt.config.get("dbt_environment")
    l2_vmfiles_prefix = f"l2_data/from_sftp_server/VMFiles/{dbt_env_name}"

    # set databricks temporary volume path based on dbt cloud environment name
    # TODO: use volume path based on dbt cloud account. current env vars listed in docs are not available
    # see https://docs.getdbt.com/docs/build/environment-variables#special-environment-variables
    if dbt_env_name == "dev":
        vol_prefix = "dbt_hugh"
    elif dbt_env_name == "prod":
        vol_prefix = "dbt"
    else:
        raise ValueError(
            f"Invalid `vol_prefix` handling of dbt environment name: {dbt_env_name}"
        )
    databricks_volume_directory = (
        f"/Volumes/goodparty_data_catalog/{vol_prefix}/object_storage/l2_temp"
    )

    # get list of states
    states: DataFrame = (
        dbt.ref("stg_airbyte_source__ballotready_s3_uscities_v1_77")
        .select("state_id")
        .distinct()
    )
    states = states.withColumn("state_id", upper(col("state_id").cast(StringType())))

    # remove Virgin Islands (VI), Puerto Rico (PR) since there's no L2 data for them
    states = states.filter(~col("state_id").isin(["VI", "PR"]))
    state_list = [row.state_id for row in states.select("state_id").collect()]
    logging.info(f"States included: {', '.join(sorted(state_list))}")

    # set function to extract and load the files with credentials and configuration
    extract_and_load_uniform = _extract_and_load_w_params(
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_user=sftp_user,
        sftp_password=sftp_password,
        s3_bucket=s3_bucket,
        s3_prefix=l2_vmfiles_prefix,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        databricks_volume_directory=databricks_volume_directory,
        is_uniform=True,
    )

    extract_and_load_non_uniform = _extract_and_load_w_params(
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_user=sftp_user,
        sftp_password=sftp_password,
        s3_bucket=s3_bucket,
        s3_prefix=l2_vmfiles_prefix,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        databricks_volume_directory=databricks_volume_directory,
        is_uniform=False,
    )

    # Convert to pandas DataFrame for processing on spark driver since sftp connection for large files
    # breaks when run through UDF on spark. `mapply` and other parallel processing libraries are
    # are not robust so stick with one core in databricks
    # TODO: move this to airflow over parallel tasks by each state
    states_pd = states.toPandas()
    uniform_loads = states_pd["state_id"].apply(extract_and_load_uniform)
    non_uniform_loads = states_pd["state_id"].apply(extract_and_load_non_uniform)
    states_pd["load_details"] = pd.concat(
        [uniform_loads, non_uniform_loads], ignore_index=True
    )

    # Convert back to Spark DataFrame
    load_details_schema = StructType(
        [
            StructField(name="state_id", dataType=StringType(), nullable=True),
            StructField(
                name="load_details",
                dataType=StructType(
                    [
                        StructField(
                            name="state_id", dataType=StringType(), nullable=True
                        ),
                        StructField("source_file_names", ArrayType(StringType()), True),
                        StructField("source_zip_file", StringType(), True),
                        StructField("loaded_at", TimestampType(), True),
                        StructField("s3_state_prefix", StringType(), True),
                    ]
                ),
                nullable=True,
            ),
        ]
    )
    states = session.createDataFrame(states_pd, load_details_schema)

    # generate an uuid for the load job
    states = states.withColumn("load_id", lit(str(uuid4())))

    # trigger preceding transformations and filter out nulls
    states.cache()
    states.count()
    states = states.filter(col("load_details.state_id").isNotNull())
    states = states.filter(col("load_details.source_file_names").isNotNull())
    states = states.filter(col("load_details.source_zip_file").isNotNull())

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

    # clean up objects inside the temp directory
    if os.path.exists(databricks_volume_directory):
        for item in os.listdir(databricks_volume_directory):
            item_path = os.path.join(databricks_volume_directory, item)
            if os.path.isfile(item_path):
                os.remove(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)

    return exploded_states
