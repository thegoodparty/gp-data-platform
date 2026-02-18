import logging
import os
import re
import time
from datetime import datetime, timezone
from tempfile import TemporaryDirectory
from typing import Any, Dict, List, Tuple
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
) -> Tuple[List[str], Dict[str, str]]:
    """
    Lists files in remote_dir matching file_pattern, downloads them locally.
    ZIP files are extracted; plain files (.tab, .csv) are kept as-is.

    Returns:
        - List of local file paths for the downloaded/extracted files.
        - Dict mapping each basename to its SFTP modification timestamp (ISO 8601).
    """
    file_list = sftp_client.listdir(remote_dir)
    pattern = re.compile(file_pattern)
    matching_files = [f for f in file_list if pattern.match(f)]

    if not matching_files:
        logger.info(f"No files matching pattern '{file_pattern}' found in {remote_dir}")
        return [], {}

    logger.info(f"Found {len(matching_files)} expired voter file(s): {matching_files}")

    downloaded_paths: List[str] = []
    file_timestamps: Dict[str, str] = {}
    for filename in matching_files:
        remote_path = f"{remote_dir}/{filename}"
        local_path = os.path.join(local_dir, filename)

        # Capture SFTP file modification time before downloading
        stat = sftp_client.stat(remote_path)
        mtime_iso = (
            datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
            if stat.st_mtime
            else None
        )

        logger.info(f"Downloading {remote_path} (modified: {mtime_iso})")
        sftp_client.get(
            remotepath=remote_path,
            localpath=local_path,
            max_concurrent_prefetch_requests=64,
        )

        if filename.lower().endswith(".zip"):
            try:
                with ZipFile(local_path, "r") as zf:
                    zf.extractall(path=local_dir)
                    for name in zf.namelist():
                        downloaded_paths.append(os.path.join(local_dir, name))
                        if mtime_iso:
                            file_timestamps[name] = mtime_iso
            except Exception as e:
                logger.error(f"Failed to extract {local_path}: {e}")
                raise
            os.remove(local_path)
        else:
            downloaded_paths.append(local_path)
            if mtime_iso:
                file_timestamps[filename] = mtime_iso

    return downloaded_paths, file_timestamps


def _extract_state_from_lalvoterid(voter_id: str) -> str:
    """Extract the 2-letter state code from a LALVOTERID (e.g. 'LALMD1207645' → 'MD')."""
    match = re.match(r"^LAL([A-Z]{2})", voter_id.upper())
    return match.group(1) if match else ""


def parse_expired_voter_ids(
    file_paths: List[str],
    state_allowlist: str = "",
) -> List[str]:
    """
    Reads extracted file(s) and returns a deduplicated list of LALVOTERID strings.

    Supports tab-delimited (.tab) and comma-delimited (.csv) files.
    Column name matching is case-insensitive (e.g. ``LALVoterID`` → ``LALVOTERID``).

    Args:
        file_paths: List of local file paths to parse.
        state_allowlist: Comma-separated state codes to filter on (e.g., "NC,WY").
            Uses ``state_postal_code`` column if present, otherwise extracts the
            state from the LALVOTERID prefix (e.g. ``LALMD...`` → ``MD``).
            Empty string = no filtering (all states).
    """
    states = (
        {s.strip().upper() for s in state_allowlist.split(",") if s.strip()}
        if state_allowlist
        else set()
    )
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

        # Normalise column names to uppercase for case-insensitive matching
        df.columns = [c.upper() for c in df.columns]

        if "LALVOTERID" not in df.columns:
            logger.warning(
                f"LALVOTERID column not found in {file_path}. "
                f"Available columns: {list(df.columns)}"
            )
            continue

        # Drop rows with missing LALVOTERID
        df = df[df["LALVOTERID"].notna() & (df["LALVOTERID"].str.strip() != "")]

        if states:
            before = len(df)
            if "STATE_POSTAL_CODE" in df.columns:
                df = df[df["STATE_POSTAL_CODE"].str.upper().isin(states)]
            else:
                df = df[
                    df["LALVOTERID"].apply(_extract_state_from_lalvoterid).isin(states)
                ]
            logger.info(f"Filtered to states {states}: {len(df)} of {before} rows")

        ids = df["LALVOTERID"].str.strip().unique().tolist()
        logger.info(f"Parsed {len(ids)} LALVOTERID(s) from {file_path}")
        all_ids.extend(ids)

    deduplicated = list(set(all_ids))
    logger.info(f"Total unique expired LALVOTERIDs: {len(deduplicated)}")
    return deduplicated


def filter_by_state_allowlist(
    lalvoterids: List[str],
    state_allowlist: str = "",
) -> List[str]:
    """
    Filter a list of LALVOTERIDs to only include those whose embedded state
    code is in the allowlist.

    Args:
        lalvoterids: List of LALVOTERID strings.
        state_allowlist: Comma-separated state codes (e.g., "NC,WY").
            Empty string = no filtering (returns input unchanged).

    Returns:
        Filtered list of LALVOTERIDs.
    """
    if not state_allowlist or not state_allowlist.strip():
        return lalvoterids

    states = {s.strip().upper() for s in state_allowlist.split(",") if s.strip()}
    filtered = [
        vid for vid in lalvoterids if _extract_state_from_lalvoterid(vid) in states
    ]
    logger.info(
        f"State allowlist {states}: {len(filtered)} of {len(lalvoterids)} LALVOTERIDs"
    )
    return filtered


def get_expired_voter_ids(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_dir: str,
    file_pattern: str,
) -> Dict[str, Any]:
    """
    High-level function that connects to SFTP, downloads expired voter files,
    and returns the LALVOTERID list along with source file names.

    Returns:
        {"lalvoterids": [...], "source_files": [...], "local_paths": [...],
         "file_timestamps": {"filename": "ISO-8601", ...}}
    """
    transport = None
    sftp_client = None
    try:
        transport, sftp_client = create_sftp_connection(
            host=host, port=port, username=username, password=password
        )

        with TemporaryDirectory(prefix="l2_expired_") as temp_dir:
            extracted_paths, file_timestamps = download_expired_voter_files(
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
                "file_timestamps": file_timestamps,
            }
    finally:
        if sftp_client is not None:
            sftp_client.close()
        if transport is not None:
            transport.close()
