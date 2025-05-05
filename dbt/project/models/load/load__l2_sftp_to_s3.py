# import os
import re

# import tempfile
# from datetime import datetime
from typing import Callable

# import pandas as pd
from paramiko import SFTPClient, Transport
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, upper
from pyspark.sql.types import StringType

# import tempfile
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

    def _extract_and_load(state_id: str) -> str:
        # def _extract_and_load(state_id: str) -> Dict[str, str | datetime | int]:
        # @pandas_udf(returnType=StringType())
        # def _extract_and_load(state_id: pd.Series) -> pd.Series:
        """
        UDF to process state IDs and extract and load files from SFTP to S3.

        Args:
            state_id: Series of state IDs

        Returns:
            JSON string with load details
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
        # Create SFTP connection inside the UDF
        transport: Transport = Transport((host, port))
        transport.connect(username=user, password=password)
        sftp_client: SFTPClient | None = SFTPClient.from_transport(transport)
        if sftp_client is None:
            raise ValueError("Failed to create SFTP client")

        try:
            # file_name_pattern = f"VM2--{state_id}*.zip"
            file_path = "/VMFiles/"
            file_list = sftp_client.listdir(file_path)
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

        finally:
            # Close SFTP connection
            sftp_client.close()
            transport.close()

        # download the file from the sftp server and extract it
        file_name = file_list[0]
        # full_file_path = os.path.join(file_path, file_name)

        # Create a temporary directory to store the zip file and extracted contents
        # with tempfile.TemporaryDirectory() as temp_dir:
        #     local_zip_path = os.path.join(temp_dir, file_name)

        #     # Download the file from the SFTP server
        #     sftp_client.get(full_file_path, local_zip_path)

        #     # Extract the zip file
        #     file_names = []
        #     with ZipFile(local_zip_path, 'r') as zip_file:
        #         file_names = zip_file.namelist()
        #         zip_file.extractall(path=temp_dir)

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

        # load_time = datetime.now().isoformat()
        # result = {
        #     "state_id": state_id,
        #     "file_names": file_names,
        #     "zip_file": file_name,
        #     "load_time": load_time,
        #     # "s3_prefix": s3_prefix
        # }

        from json import dumps

        # return dumps(result)

        return dumps({"state_id": state_id, "file_names": [file_name]})

    # return_type = MapType(StringType(), ArrayType(StringType()))
    # _extract_and_load = udf(_extract_and_load, returnType=return_type)
    _extract_and_load = udf(_extract_and_load, returnType=StringType())
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

    # for dev just take the first state
    states = states.limit(1)

    extract_and_load = _extract_and_load_w_sftp(
        sftp_host, sftp_port, sftp_user, sftp_password
    )
    states = states.withColumn("load_details", extract_and_load(states["state_id"]))

    return states
