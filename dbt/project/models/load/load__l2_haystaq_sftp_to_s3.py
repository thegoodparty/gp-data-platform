import logging
import os
import re
import time
import traceback
from collections.abc import Callable
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Any, Literal
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
) -> tuple[Transport, SFTPClient]:
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
            logging.error(f"SFTP connection attempt {attempt + 1} failed: {e!s}")
            if attempt == max_retries - 1:
                raise
            logging.warning(f"Waiting {retry_delay} seconds before next attempt...")
            time.sleep(retry_delay)
    raise Exception("Failed to establish SFTP connection after all retries")


def _select_latest_zip_file(file_list: list[str], file_pattern: re.Pattern) -> str | None:
    """
    Select the latest zip file from a list using a regex with a single (YYYYMMDD) capture group.
    """
    matches: list[tuple[str, str]] = []
    for file_name in file_list:
        match = re.match(file_pattern, file_name)
        if match:
            matches.append((file_name, match.group(1)))

    if not matches:
        return None

    # Sort by YYYYMMDD lexicographically (works for YYYYMMDD format)
    matches.sort(key=lambda t: t[1], reverse=True)
    return matches[0][0]


def _resolve_pickup_action(
    tab_file_name: str,
    s3_state_prefix: str,
    s3_keys: list[str],
    logged_file_names: set[str],
) -> tuple[Literal["download", "self_heal", "skip"], str | None]:
    """
    Decide how to handle the latest SFTP file. "In S3" alone is not proof of
    completion: a run that dies after uploading but before its sync-log append
    leaves the file in S3 with no log row, and the downstream Databricks load
    only consumes logged files. Such orphans must be re-logged ("self_heal"),
    not skipped.

    Returns (action, matched_file_name). Matching is case-insensitive, but the
    returned name is the S3 key's exact suffix after `s3_state_prefix`: S3 GETs
    are case-sensitive and downstream reads prefix + logged name verbatim, so
    only the spelling that actually exists in S3 may be logged.
    """
    tab_lower = tab_file_name.lower()
    matched_key = next(
        (key for key in s3_keys if key.lower().endswith(f"/{tab_lower}")),
        None,
    )
    if matched_key is None:
        return "download", None
    matched_file_name = matched_key[len(s3_state_prefix) :]
    if matched_file_name.lower() in {name.lower() for name in logged_file_names}:
        return "skip", matched_file_name
    return "self_heal", matched_file_name


def _locate_extracted_tab(extracted_names: list[str], temp_dir: str) -> str | None:
    """
    Return the extracted `.tab`'s local path. None means the zip listed exactly
    one tab but the file never materialized — seen with partial zips while the
    vendor is mid-upload — which callers treat as retryable (a later run gets
    the completed file). Any other member count is a contract violation and
    raises.
    """
    tab_names = [name for name in extracted_names if name.lower().endswith(".tab")]
    if len(tab_names) != 1:
        raise ValueError(f"Expected 1 .tab in zip, got {len(tab_names)}: {tab_names}")

    # `ZipFile.extractall` preserves any directory structure inside the zip, so
    # the extracted file may not live at the root of `temp_dir`.
    local_tab_path = os.path.join(temp_dir, tab_names[0])
    if not os.path.isfile(local_tab_path):
        return None
    return local_tab_path


def _collect_load_details(
    state_ids: list[str],
    extract_fns: dict[str, Callable[[str], dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Run every state through every extractor, isolating failures so one bad
    state cannot abort the rest (a mid-loop crash used to strand every
    already-uploaded file unlogged). Returns (details, failures).
    """
    all_load_details: list[dict[str, Any]] = []
    failures: list[str] = []
    for state_id in state_ids:
        for kind, extract_fn in extract_fns.items():
            try:
                result = extract_fn(state_id)
            except Exception as e:
                logging.error(f"Extraction failed for state {state_id} ({kind}): {e!s}")
                failures.append(f"{state_id} ({kind}): {e!s}")
                continue
            if result["state_id"] is not None:
                all_load_details.append({"state_id": state_id, "load_details": result})
    return all_load_details, failures


def _finalize_load_details(details: list[dict[str, Any]], failures: list[str]) -> list[dict[str, Any]]:
    """
    Persist partial progress; fail only on a total wipeout. Raising on any
    failure would discard every successful row, so one persistently bad state
    could starve the other states' log rows indefinitely. When at least one
    extraction succeeded, log the failures and return the successes (failed
    states retry next run: their files are re-pulled or self-healed). When
    every extraction failed, something systemic broke (e.g. SFTP outage);
    there is nothing to persist, so fail the run loudly.
    """
    if failures and not details:
        raise RuntimeError(f"all {len(failures)} extraction(s) failed: {'; '.join(failures)}")
    if failures:
        logging.error(
            f"{len(failures)} state extraction(s) failed and will retry next run: {'; '.join(failures)}"
        )
    return details


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
    logged_file_names: set[str],
) -> Callable[[str], dict[str, Any]]:
    """
    Creates a function that downloads a single state's Haystaq zip from SFTP, extracts the `.tab`,
    and uploads it to S3.
    """

    def _extract_and_load(state_id: str) -> dict[str, Any]:
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
            zip_pattern_str = "^" + state_lower + "_" + suffix + r"_(\d{8})\.tab\.zip$"
            zip_pattern = re.compile(zip_pattern_str, flags=re.IGNORECASE)

            try:
                file_list = sftp_client.listdir(remote_dir)
            except FileNotFoundError:
                logging.error(f"SFTP directory not found: {remote_dir}. Skipping {state_id}.")
                return EMPTY_LOAD_DETAILS

            source_zip_file_name = _select_latest_zip_file(
                file_list=file_list,
                file_pattern=zip_pattern,
            )
            if source_zip_file_name is None:
                logging.warning(f"No Haystaq {haystaq_kind} zip found for state {state_id} in {remote_dir}")
                return EMPTY_LOAD_DETAILS

            # We upload the extracted `.tab` to S3 (not the zip).
            tab_file_name = re.sub(r"\.zip$", "", source_zip_file_name, flags=re.I)

            s3_state_prefix = f"{s3_prefix}/{state_id.upper()}/{haystaq_kind}/"
            s3_file_list = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_state_prefix)
            s3_keys = [f["Key"] for f in s3_file_list.get("Contents", [])]
            action, matched_file_name = _resolve_pickup_action(
                tab_file_name, s3_state_prefix, s3_keys, logged_file_names
            )
            if action == "skip":
                logging.info(
                    f"{haystaq_kind} file already in S3 and logged for {state_id}: {matched_file_name}"
                )
                return EMPTY_LOAD_DETAILS
            if action == "self_heal":
                logging.warning(
                    f"{haystaq_kind} file for {state_id} is in S3 but has no sync-log row "
                    f"(a prior run died before logging it); re-logging without re-download: {matched_file_name}"
                )
                return {
                    "state_id": state_id,
                    "source_file_names": [matched_file_name],
                    "source_zip_file": source_zip_file_name,
                    "loaded_at": datetime.now(),
                    "s3_state_prefix": s3_state_prefix,
                }

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
                    logging.error(f"Source zip {full_zip_path} locked: {e!s}. Skipping for now.")
                    return EMPTY_LOAD_DETAILS

                try:
                    with ZipFile(local_zip_path, "r") as zip_file:
                        extracted_names = zip_file.namelist()
                        zip_file.extractall(path=temp_dir)
                except Exception:
                    logging.error(f"Failed to extract {local_zip_path}. Skipping for now.")
                    return EMPTY_LOAD_DETAILS

                local_tab_path = _locate_extracted_tab(extracted_names, temp_dir)
                if local_tab_path is None:
                    # Zip was likely partial (vendor mid-upload); retry on a later run.
                    logging.error(
                        f"Extracted tab missing for {state_id} ({haystaq_kind}) from "
                        f"{source_zip_file_name}. Skipping for now."
                    )
                    return EMPTY_LOAD_DETAILS

                extracted_tab_name = os.path.basename(local_tab_path)
                if extracted_tab_name.lower() != tab_file_name.lower():
                    # Upload under the zip-derived name regardless: every future
                    # run derives that name from the SFTP listing to decide
                    # skip/self-heal, so uploading the extracted spelling would
                    # never be found again and the file would re-download and
                    # re-log on every run.
                    logging.warning(
                        f"Extracted tab name {extracted_tab_name} does not match expected "
                        f"{tab_file_name}; uploading under the expected name."
                    )

                s3_key = f"{s3_state_prefix}{tab_file_name}"
                s3_client.upload_file(Filename=local_tab_path, Bucket=s3_bucket, Key=s3_key)

                # Delete older versions for this state/type (keep only the latest by filename)
                tab_pattern_str = "^" + state_lower + "_" + suffix + r"_(\d{8})\.tab$"
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
            logging.error(f"Error processing state {state_id} ({haystaq_kind}): {e!s}")
            logging.error(f"Full exception details:\n{traceback.format_exc()}")
            # Re-raise unwrapped: the caller prefixes state/kind onto str(e) for
            # the end-of-run summary, and wrapping here would duplicate that
            # context and stuff the whole traceback into the summary line.
            raise
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
        # Stateful sync log: a full refresh re-lists SFTP and keeps a row only for states with a
        # brand-new file, silently dropping the accumulated history for every other state (and
        # orphaning their downstream source tables). Pin full_refresh off so --full-refresh, incl.
        # the on-merge state:modified+ build, can never wipe it.
        full_refresh=False,
        tags=["l2", "haystaq", "sftp", "s3", "load"],
    )

    # sftp server configuration
    sftp_host = dbt.config.meta_get("l2_sftp_host")
    sftp_port = int(dbt.config.meta_get("l2_sftp_port"))
    sftp_user = dbt.config.meta_get("l2_sftp_user")
    dbt_env_name = dbt.config.meta_get("dbt_environment")
    sftp_password = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env_name}", key="l2-sftp-password"
    )

    flags_remote_dir = dbt.config.meta_get(
        "l2_haystaq_flags_sftp_dir", "/L2-Haystaq Issue Model Flags for Voters"
    )
    scores_remote_dir = dbt.config.meta_get("l2_haystaq_scores_sftp_dir", "/L2-Haystaq Issue Model Scores")
    state_allowlist_raw = dbt.config.meta_get("l2_state_allowlist")

    # S3 configuration
    s3_bucket = dbt.config.meta_get("l2_s3_bucket")
    s3_access_key = dbt.config.meta_get("l2_s3_access_key")
    s3_secret_key = dbutils.secrets.get(  # type: ignore[name-defined]
        scope=f"dbt-secrets-{dbt_env_name}", key="s3-secret-key"
    )
    l2_haystaq_prefix = f"l2_data/from_sftp_server/Haystaq/{dbt_env_name}"

    databricks_volume_directory = f"/Volumes/goodparty_data_catalog/{dbt.this.schema}/object_storage/l2_temp"

    # get list of states
    states: DataFrame = (
        dbt.ref("stg_airbyte_source__ballotready_s3_uscities_v1_77").select("state_id").distinct()
    )
    states = states.withColumn("state_id", upper(col("state_id").cast(StringType())))

    # remove Virgin Islands (VI), Puerto Rico (PR) since there's no L2 data for them
    states = states.filter(~col("state_id").isin(["VI", "PR"]))
    if state_allowlist_raw:
        allowlist = [
            token.strip().upper() for token in re.split(r"[,\s]+", state_allowlist_raw) if token.strip()
        ]
        logging.info(f"Filtering to states in allowlist: {allowlist}")
        states = states.filter(col("state_id").isin(allowlist))
    state_list = [row.state_id for row in states.select("state_id").collect()]
    logging.info(f"States included: {', '.join(sorted(state_list))}")

    # "Already handled" must mean "logged", not merely "uploaded to S3" — the
    # downstream Databricks load only consumes logged files. Read this model's
    # own log so files stranded in S3 by an interrupted run get re-logged.
    logged_file_names: set[str] = set()
    if dbt.is_incremental:
        logged_file_names = {
            row.source_file_name.lower()
            for row in session.table(f"{dbt.this}").select("source_file_name").collect()
            if row.source_file_name is not None
        }

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
        logged_file_names=logged_file_names,
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
        logged_file_names=logged_file_names,
    )

    all_load_details, failures = _collect_load_details(
        state_ids=state_list,
        extract_fns={"flags": extract_flags, "scores": extract_scores},
    )
    all_load_details = _finalize_load_details(all_load_details, failures)

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
