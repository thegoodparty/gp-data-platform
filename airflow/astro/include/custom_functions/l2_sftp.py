import logging
import os
import re
import time
from tempfile import TemporaryDirectory
from typing import Dict, List
from zipfile import ZipFile

import pandas as pd
from paramiko import SFTPClient, Transport

logger = logging.getLogger("airflow.task")


def create_sftp_connection(
    host: str,
    port: int,
    username: str,
    password: str,
    max_retries: int = 3,
    retry_delay: int = 5,
) -> tuple[Transport, SFTPClient]:
    """
    Creates an SFTP connection with retry logic and keep-alive settings.

    Mirrors the pattern from dbt/project/models/load/load__l2_sftp_to_s3.py.
    """
    for attempt in range(max_retries):
        try:
            transport = Transport((host, port))
            transport.set_keepalive(30)
            transport.connect(username=username, password=password)
            sftp_client = SFTPClient.from_transport(transport)
            if sftp_client is None:
                raise ValueError("Failed to create SFTP client")
            logger.info("SFTP client created successfully")
            return transport, sftp_client
        except Exception as e:
            logger.error(f"SFTP connection attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Waiting {retry_delay} seconds before next attempt...")
            time.sleep(retry_delay)
    raise Exception("Failed to establish SFTP connection after all retries")


def download_expired_voter_files(
    sftp_client: SFTPClient,
    remote_dir: str,
    file_pattern: str,
    local_dir: str,
) -> List[str]:
    """
    Lists files in remote_dir matching file_pattern, downloads and extracts ZIPs.

    Returns a list of local file paths for the extracted files.
    """
    file_list = sftp_client.listdir(remote_dir)
    pattern = re.compile(file_pattern)
    matching_files = [f for f in file_list if pattern.match(f)]

    if not matching_files:
        logger.info(f"No files matching pattern '{file_pattern}' found in {remote_dir}")
        return []

    logger.info(f"Found {len(matching_files)} expired voter file(s): {matching_files}")

    extracted_paths: List[str] = []
    for zip_filename in matching_files:
        remote_path = os.path.join(remote_dir, zip_filename)
        local_zip_path = os.path.join(local_dir, zip_filename)

        logger.info(f"Downloading {remote_path}")
        sftp_client.get(
            remotepath=remote_path,
            localpath=local_zip_path,
            max_concurrent_prefetch_requests=64,
        )

        # Extract ZIP contents
        try:
            with ZipFile(local_zip_path, "r") as zf:
                zf.extractall(path=local_dir)
                for name in zf.namelist():
                    extracted_paths.append(os.path.join(local_dir, name))
        except Exception as e:
            logger.error(f"Failed to extract {local_zip_path}: {e}")
            raise

        # Clean up the ZIP after extraction
        os.remove(local_zip_path)

    return extracted_paths


def parse_expired_voter_ids(file_paths: List[str]) -> List[str]:
    """
    Reads extracted file(s) and returns a deduplicated list of LALVOTERID strings.

    Supports tab-delimited (.tab) and comma-delimited (.csv) files.
    Expects LALVOTERID as a column in the file.
    """
    all_ids: List[str] = []

    for file_path in file_paths:
        if not os.path.isfile(file_path):
            logger.warning(f"Skipping non-file path: {file_path}")
            continue

        delimiter = "\t" if file_path.endswith(".tab") else ","
        try:
            df = pd.read_csv(file_path, delimiter=delimiter, dtype=str)
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            raise

        if "LALVOTERID" not in df.columns:
            logger.warning(
                f"LALVOTERID column not found in {file_path}. "
                f"Available columns: {list(df.columns)}"
            )
            continue

        ids = df["LALVOTERID"].dropna().unique().tolist()
        logger.info(f"Parsed {len(ids)} LALVOTERID(s) from {file_path}")
        all_ids.extend(ids)

    deduplicated = list(set(all_ids))
    logger.info(f"Total unique expired LALVOTERIDs: {len(deduplicated)}")
    return deduplicated


def get_expired_voter_ids(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_dir: str,
    file_pattern: str,
) -> Dict[str, List[str]]:
    """
    High-level function that connects to SFTP, downloads expired voter files,
    and returns the LALVOTERID list along with source file names.

    Returns:
        {"lalvoterids": [...], "source_files": [...], "local_paths": [...]}
    """
    transport = None
    sftp_client = None
    try:
        transport, sftp_client = create_sftp_connection(
            host=host, port=port, username=username, password=password
        )

        with TemporaryDirectory(prefix="l2_expired_") as temp_dir:
            extracted_paths = download_expired_voter_files(
                sftp_client=sftp_client,
                remote_dir=remote_dir,
                file_pattern=file_pattern,
                local_dir=temp_dir,
            )

            if not extracted_paths:
                return {"lalvoterids": [], "source_files": [], "local_paths": []}

            lalvoterids = parse_expired_voter_ids(extracted_paths)

            # Collect source file basenames for logging
            source_files = [os.path.basename(p) for p in extracted_paths]

            return {
                "lalvoterids": lalvoterids,
                "source_files": source_files,
                "local_paths": extracted_paths,
            }
    finally:
        if sftp_client is not None:
            sftp_client.close()
        if transport is not None:
            transport.close()
