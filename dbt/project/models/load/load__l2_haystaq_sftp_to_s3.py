import logging
import os
import re
import time
import traceback
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple
from uuid import uuid4
from zipfile import ZipFile

import boto3
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
) -> Tuple[Transport, SFTPClient]:
    for attempt in range(max_retries):
        try:
            transport = Transport((host, port))
            transport.set_keepalive(30)
            transport.connect(username=username, password=password)
            sftp_client = SFTPClient.from_transport(transport)
            if sftp_client is None:
                raise ValueError("Failed to create SFTP client")
            return transport, sftp_client
        except Exception as e:
            logging.error(f"SFTP connection attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise
            logging.warning(f"Waiting {retry_delay} seconds before next attempt...")
            time.sleep(retry_delay)
    raise Exception("Failed to establish SFTP connection after all retries")


def _select_latest_zip_file(
    file_list: List[str], file_pattern: re.Pattern
) -> Optional[str]:
    """
    Select the latest zip file from a list using a regex with a single (YYYYMMDD) capture group.
    """
    matches: List[Tuple[str, str]] = []
    for file_name in file_list:
        match = re.match(file_pattern, file_name)
        if match:
            matches.append((file_name, match.group(1)))

    if not matches:
        return None

    # Sort by YYYYMMDD lexicographically (works for YYYYMMDD format)
    matches.sort(key=lambda t: t[1], reverse=True)
    return matches[0][0]


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
    remote_dir: str,
    haystaq_kind: Literal["flags", "scores"],
) -> Callable[[str], Dict[str, Any]]:
    """
    Creates a function that downloads a single state's Haystaq zip from SFTP, extracts the `.tab`,
    and uploads it to S3.
    """

    def _extract_and_load(state_id: str) -> Dict[str, Any]:
        logging.info(f"Processing state: {state_id} ({haystaq_kind})")
        transport = None
        sftp_client = None
        try:
            transport, sftp_client = _create_sftp_connection(
                host=sftp_host,
                port=sftp_port,
                username=sftp_user,
                password=sftp_password,
            )

            s3_client = boto3.client(
                "s3",
                aws_access_key_id=s3_access_key,
                aws_secret_access_key=s3_secret_key,
            )

            # Files look like: ak_haystaqdnaflags_20251005.tab.zip
            state_lower = state_id.lower()
            suffix = "haystaqdnaflags" if haystaq_kind == "flags" else "haystaqdnascores"
            zip_pattern_str = (
                "^" + state_lower + "_" + suffix + r"_(\d{8})\.tab\.zip$"
            )
            zip_pattern = re.compile(zip_pattern_str, flags=re.IGNORECASE)

            try:
                file_list = sftp_client.listdir(remote_dir)
            except FileNotFoundError:
                logging.error(
                    f"SFTP directory not found: {remote_dir}. Skipping {state_id}."
                )
                return EMPTY_LOAD_DETAILS

            source_zip_file_name = _select_latest_zip_file(
                file_list=file_list,
                file_pattern=zip_pattern,
            )
            if source_zip_file_name is None:
                logging.warning(
                    f"No Haystaq {haystaq_kind} zip found for state {state_id} in {remote_dir}"
                )
                return EMPTY_LOAD_DETAILS

            # We upload the extracted `.tab` to S3 (not the zip).
            tab_file_name = re.sub(r"\.zip$", "", source_zip_file_name, flags=re.I)

            s3_state_prefix = f"{s3_prefix}/{state_id.upper()}/{haystaq_kind}/"
            s3_file_list = s3_client.list_objects_v2(
                Bucket=s3_bucket, Prefix=s3_state_prefix
            )
            s3_keys = [f["Key"] for f in s3_file_list.get("Contents", [])]
            if any(key.endswith(f"/{tab_file_name}") for key in s3_keys):
                logging.info(
                    f"{haystaq_kind} file already exists in S3 for {state_id}: {tab_file_name}"
                )
                return EMPTY_LOAD_DETAILS

            full_zip_path = os.path.join(remote_dir, source_zip_file_name)

            try:
                temp_dir_ctx = TemporaryDirectory(
                    prefix=f"temp_{state_id}_{haystaq_kind}_",
                    dir=databricks_volume_directory,
                )
            except PermissionError:
                # Some environments/users don't have write perms on the configured volume path.
                # Fall back to default temp directory on the driver node.
                logging.warning(
                    f"Permission denied for temp dir {databricks_volume_directory}; falling back to default temp directory."
                )
                temp_dir_ctx = TemporaryDirectory(prefix=f"temp_{state_id}_{haystaq_kind}_")

            with temp_dir_ctx as temp_dir:
                local_zip_path = os.path.join(temp_dir, source_zip_file_name)

                try:
                    sftp_client.get(
                        remotepath=full_zip_path,
                        localpath=local_zip_path,
                        max_concurrent_prefetch_requests=64,
                    )
                except OSError as e:
                    logging.error(
                        f"Source zip {full_zip_path} locked: {str(e)}. Skipping for now."
                    )
                    return EMPTY_LOAD_DETAILS

                try:
                    with ZipFile(local_zip_path, "r") as zip_file:
                        extracted_names = zip_file.namelist()
                        zip_file.extractall(path=temp_dir)
                except Exception:
                    logging.error(
                        f"Failed to extract {local_zip_path}. Skipping for now."
                    )
                    return EMPTY_LOAD_DETAILS

                # Expect exactly one .tab file
                tab_names = [
                    name for name in extracted_names if name.lower().endswith(".tab")
                ]
                if len(tab_names) != 1:
                    raise ValueError(
                        f"Expected 1 .tab in {source_zip_file_name}, got {len(tab_names)}: {tab_names}"
                    )

                extracted_tab_name = os.path.basename(tab_names[0])
                if extracted_tab_name.lower() != tab_file_name.lower():
                    logging.warning(
                        f"Extracted tab name {extracted_tab_name} does not match expected {tab_file_name}; uploading extracted name."
                    )
                    tab_file_name = extracted_tab_name

                local_tab_path = os.path.join(temp_dir, extracted_tab_name)
                if not os.path.isfile(local_tab_path):
                    raise FileNotFoundError(
                        f"Extracted tab file not found at {local_tab_path}"
                    )

                s3_key = f"{s3_state_prefix}{tab_file_name}"
                s3_client.upload_file(Filename=local_tab_path, Bucket=s3_bucket, Key=s3_key)

                # Delete older versions for this state/type (keep only the latest by filename)
                tab_pattern_str = (
                    "^" + state_lower + "_" + suffix + r"_(\d{8})\.tab$"
                )
                tab_pattern = re.compile(tab_pattern_str, flags=re.IGNORECASE)
                for key in s3_keys:
                    key_basename = os.path.basename(key)
                    if re.match(tab_pattern, key_basename) and key_basename.lower() != tab_file_name.lower():
                        s3_client.delete_object(Bucket=s3_bucket, Key=key)

            return {
                "state_id": state_id,
                "source_file_names": [tab_file_name],
                "source_zip_file": source_zip_file_name,
                "loaded_at": datetime.now(),
                "s3_state_prefix": s3_state_prefix,
            }

        except Exception as e:
            logging.error(f"Error processing state {state_id} ({haystaq_kind}): {str(e)}")
            error_details = traceback.format_exc()
            logging.error(f"Full exception details:\n{error_details}")
            raise Exception(
                f"Error processing state {state_id} ({haystaq_kind}): {str(e)}\nFull traceback:\n{error_details}"
            )
        finally:
            if sftp_client is not None:
                sftp_client.close()
            if transport is not None:
                transport.close()

    return _extract_and_load


def model(dbt, session: SparkSession) -> DataFrame:
    dbt.config(
        submission_method="all_purpose_cluster",
        http_path="sql/protocolv1/o/3578414625112071/0409-211859-6hzpukya",
        materialized="incremental",
        incremental_strategy="append",
        unique_key="id",
        on_schema_change="fail",
        tags=["l2", "haystaq", "sftp", "s3", "load"],
    )

    # sftp server configuration
    sftp_host = dbt.config.get("l2_sftp_host")
    sftp_port = int(dbt.config.get("l2_sftp_port"))
    sftp_user = dbt.config.get("l2_sftp_user")
    dbt_env_name = dbt.config.get("dbt_environment")
    sftp_password = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env_name}", key="l2-sftp-password"
    )

    flags_remote_dir = dbt.config.get(
        "l2_haystaq_flags_sftp_dir", "/L2-Haystaq Issue Model Flags for Voters"
    )
    scores_remote_dir = dbt.config.get(
        "l2_haystaq_scores_sftp_dir", "/L2-Haystaq Issue Model Scores"
    )
    state_allowlist_raw = dbt.config.get("l2_state_allowlist")

    # S3 configuration
    s3_bucket = dbt.config.get("l2_s3_bucket")
    s3_access_key = dbt.config.get("l2_s3_access_key")
    s3_secret_key = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env_name}", key="s3-secret-key"
    )
    l2_haystaq_prefix = f"l2_data/from_sftp_server/Haystaq/{dbt_env_name}"

    # set databricks temporary volume path based on dbt cloud environment name
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
    if state_allowlist_raw:
        allowlist = [
            token.strip().upper()
            for token in re.split(r"[,\s]+", state_allowlist_raw)
            if token.strip()
        ]
        logging.info(f"Filtering to states in allowlist: {allowlist}")
        states = states.filter(col("state_id").isin(allowlist))
    state_list = [row.state_id for row in states.select("state_id").collect()]
    logging.info(f"States included: {', '.join(sorted(state_list))}")

    extract_flags = _extract_and_load_w_params(
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_user=sftp_user,
        sftp_password=sftp_password,
        s3_bucket=s3_bucket,
        s3_prefix=l2_haystaq_prefix,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        databricks_volume_directory=databricks_volume_directory,
        remote_dir=flags_remote_dir,
        haystaq_kind="flags",
    )

    extract_scores = _extract_and_load_w_params(
        sftp_host=sftp_host,
        sftp_port=sftp_port,
        sftp_user=sftp_user,
        sftp_password=sftp_password,
        s3_bucket=s3_bucket,
        s3_prefix=l2_haystaq_prefix,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
        databricks_volume_directory=databricks_volume_directory,
        remote_dir=scores_remote_dir,
        haystaq_kind="scores",
    )

    all_load_details: List[Dict[str, Any]] = []
    for state_id in state_list:
        flags_result = extract_flags(state_id)
        if flags_result["state_id"] is not None:
            all_load_details.append({"state_id": state_id, "load_details": flags_result})

        scores_result = extract_scores(state_id)
        if scores_result["state_id"] is not None:
            all_load_details.append(
                {"state_id": state_id, "load_details": scores_result}
            )

    # schema matches `load__l2_sftp_to_s3`
    load_details_schema = StructType(
        [
            StructField(name="state_id", dataType=StringType(), nullable=True),
            StructField(
                name="load_details",
                dataType=StructType(
                    [
                        StructField(name="state_id", dataType=StringType(), nullable=True),
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

    states_loaded = session.createDataFrame(all_load_details, load_details_schema)
    states_loaded = states_loaded.withColumn("load_id", lit(str(uuid4())))

    # trigger preceding transformations and filter out nulls
    states_loaded.cache()
    states_loaded.count()
    states_loaded = states_loaded.filter(col("load_details.state_id").isNotNull())
    states_loaded = states_loaded.filter(col("load_details.source_file_names").isNotNull())
    states_loaded = states_loaded.filter(col("load_details.source_zip_file").isNotNull())

    exploded_states = states_loaded.select(
        col("load_id"),
        col("load_details.loaded_at").alias("loaded_at"),
        explode(col("load_details.source_file_names")).alias("source_file_name"),
        col("load_details.source_zip_file").alias("source_zip_file"),
        col("load_details.state_id").alias("state_id"),
        col("load_details.s3_state_prefix").alias("s3_state_prefix"),
    )

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

    return exploded_states
