"""L2 file handling for the load_l2_voter_files DAG.

Nothing is recorded between runs: the SFTP listing versus S3 decides what to copy, and S3 versus
each table's last-altered time decides what to load.

Staged files live at `{prefix}/{folder}/{file}`, where folder is a state for the voter archives
and EXPIRED_FOLDER for the expired-ID file.
"""

import io
import logging
import os
import re
import shutil
from datetime import UTC, datetime
from zipfile import ZipFile

import pandas as pd
from boto3.s3.transfer import TransferConfig
from include.custom_functions.l2_sftp import download, list_matching
from paramiko import SFTPClient

logger = logging.getLogger("airflow.task")

EXPIRED_FOLDER = "EXPIRED"
EXPIRED_TABLE = "l2_s3_expired_voters"

# The per-state archives. Members are known from the archive name, so a run that died part way
# is detectable without opening anything.
ARCHIVE_GROUPS: dict[str, dict] = {
    "uniform": {
        "remote_dir": "/VM2Uniform",
        "pattern": re.compile(r"^VM2Uniform--(?P<folder>[A-Z]{2})--\d{4}-\d{2}-\d{2}\.zip$"),
        "members": ("{base}.tab", "{base}_DataDictionary.csv"),
    },
    "vm2": {
        "remote_dir": "/VMFiles",
        "pattern": re.compile(r"^VM2--(?P<folder>[A-Z]{2})--\d{4}-\d{2}-\d{2}\.zip$"),
        "members": (
            "{base}-DEMOGRAPHIC.tab",
            "{base}-DEMOGRAPHIC_DataDictionary.csv",
            "{base}-VOTEHISTORY.tab",
            "{base}-VOTEHISTORY_DataDictionary.csv",
        ),
    },
}

_DATE = r"\d{4}-\d{2}-\d{2}"
_SOURCE_FILE_TYPES: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(rf"^VM2Uniform--[A-Z]{{2}}--{_DATE}\.tab$"), "uniform"),
    (re.compile(rf"^VM2Uniform--[A-Z]{{2}}--{_DATE}_DataDictionary\.csv$"), "uniform_data_dictionary"),
    (re.compile(rf"^VM2--[A-Z]{{2}}--{_DATE}-DEMOGRAPHIC\.tab$"), "demographic"),
    (
        re.compile(rf"^VM2--[A-Z]{{2}}--{_DATE}-DEMOGRAPHIC_DataDictionary\.csv$"),
        "demographic_data_dictionary",
    ),
    (re.compile(rf"^VM2--[A-Z]{{2}}--{_DATE}-VOTEHISTORY\.tab$"), "vote_history"),
    (
        re.compile(rf"^VM2--[A-Z]{{2}}--{_DATE}-VOTEHISTORY_DataDictionary\.csv$"),
        "vote_history_data_dictionary",
    ),
)
_STATE_FOLDER = re.compile(r"^[A-Z]{2}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
# Staged names reach a SQL string literal, so anything quoted or path-like is rejected.
_SAFE_FILE_NAME = re.compile(r"^[A-Za-z0-9._-]+$")

# L2 pads its data dictionaries with a preamble and a trailing legend; VOTEHISTORY's is shorter.
_DICTIONARY_HEADER_ROWS = 15
_DICTIONARY_FOOTER_ROWS = {"VOTEHISTORY": 4}
_DICTIONARY_DEFAULT_FOOTER_ROWS = 24

# 16 MiB parts stay under S3's 10,000-part ceiling for the largest members.
_TRANSFER_CONFIG = TransferConfig(multipart_chunksize=16 * 1024 * 1024, max_concurrency=4)


class _SequentialReader:
    """Exposes only read(), so boto3 streams a zip member instead of seeking.

    ZipExtFile emulates seek by re-decompressing from the start of the member, which boto3's
    seekable upload path would trigger once per part.
    """

    def __init__(self, handle):
        self._handle = handle

    def read(self, size=-1) -> bytes:
        return self._handle.read(size)


def source_file_type(source_file_name: str) -> str | None:
    """Classify a staged voter file, or None. Doubles as the destination table suffix."""
    for pattern, file_type in _SOURCE_FILE_TYPES:
        if pattern.match(source_file_name):
            return file_type
    return None


# --------------------------------------------------------------------------------------------
# SFTP -> S3
# --------------------------------------------------------------------------------------------


def _trim_data_dictionary(raw: bytes, member: str) -> bytes:
    footer_rows = _DICTIONARY_DEFAULT_FOOTER_ROWS
    for marker, rows in _DICTIONARY_FOOTER_ROWS.items():
        if marker in member:
            footer_rows = rows
    frame = pd.read_csv(
        io.BytesIO(raw), skiprows=_DICTIONARY_HEADER_ROWS, skipfooter=footer_rows, engine="python"
    )
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False)
    return buffer.getvalue().encode()


def _source(remote_dir: str, attributes, folder: str, members: list[str] | None) -> dict:
    return {
        "folder": folder,
        "remote_path": f"{remote_dir}/{attributes.filename}",
        "file_name": attributes.filename,
        "members": members,
        "size_bytes": attributes.st_size,
        "modified_at": datetime.fromtimestamp(attributes.st_mtime or 0, tz=UTC).isoformat(),
    }


def list_remote_sources(sftp_client: SFTPClient, expired_dir: str, expired_pattern: str) -> list[dict]:
    """Every L2 file we mirror: the per-state archives plus the expired-ID file.

    A state L2 has pulled while rebuilding is simply absent, not an error.
    """
    sources: list[dict] = []
    for spec in ARCHIVE_GROUPS.values():
        for attributes in list_matching(sftp_client, spec["remote_dir"], spec["pattern"]):
            base = attributes.filename.removesuffix(".zip")
            sources.append(
                _source(
                    remote_dir=spec["remote_dir"],
                    attributes=attributes,
                    folder=spec["pattern"].match(attributes.filename)["folder"],
                    members=[name.format(base=base) for name in spec["members"]],
                )
            )

    for attributes in list_matching(sftp_client, expired_dir, re.compile(expired_pattern)):
        # The expired file's members aren't derivable from its name, so they're discovered on sync.
        sources.append(_source(expired_dir, attributes, EXPIRED_FOLDER, members=None))

    return sorted(sources, key=lambda source: source["remote_path"])


def list_s3_objects(s3_client, bucket: str, prefix: str) -> dict[str, datetime]:
    """Every staged object's last-modified time, keyed by `{folder}/{file name}`."""
    staged: dict[str, datetime] = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            staged[obj["Key"].removeprefix(f"{prefix}/")] = obj["LastModified"]
    return staged


def plan_transfers(sources: list[dict], staged: dict[str, datetime]) -> list[dict]:
    """The sources S3 is missing or holds an older copy of.

    Where the members are known, requiring all of them retries a run that died mid-archive.
    """
    pending = []
    for source in sources:
        modified_at = datetime.fromisoformat(source["modified_at"])
        if source["members"] is not None:
            staged_at = [staged.get(f"{source['folder']}/{member}") for member in source["members"]]
        else:
            staged_at = [
                timestamp for key, timestamp in staged.items() if key.startswith(f"{source['folder']}/")
            ] or [None]
        if all(timestamp is not None and timestamp >= modified_at for timestamp in staged_at):
            continue
        pending.append(source)
    return pending


def _upload(s3_client, bucket: str, key: str, handle, member: str) -> None:
    logger.info(f"Uploading s3://{bucket}/{key}")
    if member.endswith("_DataDictionary.csv"):
        s3_client.put_object(Bucket=bucket, Key=key, Body=_trim_data_dictionary(handle.read(), member))
    else:
        s3_client.upload_fileobj(_SequentialReader(handle), bucket, key, Config=_TRANSFER_CONFIG)


def sync_source(
    sftp_client: SFTPClient,
    s3_client,
    bucket: str,
    prefix: str,
    source: dict,
    staging_dir: str,
) -> list[str]:
    """Download one source file and upload it, or the members we keep, into `{prefix}/{folder}/`."""
    folder_prefix = f"{prefix}/{source['folder']}/"
    local_path = os.path.join(staging_dir, source["file_name"])

    # Astro workers have a fixed 10 GiB of ephemeral storage, so a growing archive will one day
    # outgrow it. Fail with the numbers rather than filling the disk mid-download.
    free_bytes = shutil.disk_usage(staging_dir).free
    if free_bytes < source["size_bytes"] * 1.1:
        raise ValueError(
            f"{source['file_name']} needs {source['size_bytes'] / 1024**3:.1f} GB but only "
            f"{free_bytes / 1024**3:.1f} GB is free on {staging_dir}"
        )

    logger.info(f"Downloading {source['remote_path']} ({source['size_bytes'] / 1024**3:.2f} GB)")
    download(sftp_client, source["remote_path"], local_path)
    try:
        if not source["file_name"].lower().endswith(".zip"):
            with open(local_path, "rb") as handle:
                _upload(
                    s3_client, bucket, f"{folder_prefix}{source['file_name']}", handle, source["file_name"]
                )
            return [f"{folder_prefix}{source['file_name']}"]

        with ZipFile(local_path) as zip_file:
            contents = [name for name in zip_file.namelist() if not name.endswith("/")]
            members = source["members"] or contents
            missing = [member for member in members if member not in contents]
            if missing:
                raise ValueError(f"{source['file_name']} did not contain {missing}")

            for member in members:
                with zip_file.open(member) as handle:
                    _upload(s3_client, bucket, f"{folder_prefix}{member}", handle, member)
            return [f"{folder_prefix}{member}" for member in members]
    finally:
        os.remove(local_path)


# --------------------------------------------------------------------------------------------
# S3 -> Databricks
# --------------------------------------------------------------------------------------------


def plan_loads(staged: dict[str, datetime], loaded_at: dict[str, datetime]) -> list[dict]:
    """The newest staged file per table, minus the tables already built from it.

    S3 keeps every dated snapshot, so only the newest is a candidate.
    """
    latest: dict[str, tuple[str, str, datetime]] = {}
    for relative_key, staged_at in staged.items():
        folder, _, file_name = relative_key.partition("/")
        if not _SAFE_FILE_NAME.match(file_name):
            continue
        if folder == EXPIRED_FOLDER:
            table_name = EXPIRED_TABLE
        elif _STATE_FOLDER.match(folder) and (file_type := source_file_type(file_name)):
            table_name = f"l2_s3_{folder.lower()}_{file_type}"
        else:
            continue
        if table_name not in latest or staged_at > latest[table_name][2]:
            latest[table_name] = (folder, file_name, staged_at)

    return [
        {"folder": folder, "source_file_name": file_name, "table_name": table_name}
        for table_name, (folder, file_name, staged_at) in sorted(latest.items())
        if loaded_at.get(table_name) is None or loaded_at[table_name] < staged_at
    ]


def _identifier(value: str, label: str) -> str:
    if not _IDENTIFIER.match(value):
        raise ValueError(f"Unsafe {label}: {value!r}")
    return value


def build_load_sql(catalog: str, schema: str, bucket: str, prefix: str, load: dict) -> str:
    """The CTAS that rebuilds one table from its staged file.

    Every column is read as a string: the raw files carry zero-padded codes (ZIPs, ZipPlus4, DPBC)
    that type inference coerces to int, dropping leading zeros before any model can see them.
    """
    file_name = load["source_file_name"]
    if not _SAFE_FILE_NAME.match(file_name):
        raise ValueError(f"Unsafe source file name: {file_name!r}")

    table = (
        f"`{_identifier(catalog, 'catalog')}`"
        f".`{_identifier(schema, 'schema')}`"
        f".`{_identifier(load['table_name'], 'table name')}`"
    )
    delimiter = "\\t" if file_name.endswith(".tab") else ","
    s3_path = f"s3://{bucket}/{prefix}/{load['folder']}/{file_name}"
    return (
        f"CREATE OR REPLACE TABLE {table} "
        "CLUSTER BY AUTO "
        "AS SELECT * EXCEPT (_rescued_data), current_timestamp() AS loaded_at "
        f"FROM read_files('{s3_path}', format => 'csv', delimiter => '{delimiter}', "
        "header => true, inferColumnTypes => false)"
    )


def get_table_loaded_at(connection, catalog: str, schema: str) -> dict[str, datetime]:
    """When each table in the target schema was last rebuilt."""
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"SELECT table_name, last_altered FROM `{_identifier(catalog, 'catalog')}`"
            f".information_schema.tables WHERE table_schema = '{_identifier(schema, 'schema')}'"
        )
        return {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        cursor.close()


def load_table(connection, catalog: str, schema: str, bucket: str, prefix: str, load: dict) -> str:
    """Rebuild one table from its staged file, returning its fully qualified name."""
    sql = build_load_sql(catalog, schema, bucket, prefix, load)
    logger.info(sql)
    cursor = connection.cursor()
    try:
        cursor.execute(
            f"CREATE SCHEMA IF NOT EXISTS `{_identifier(catalog, 'catalog')}`"
            f".`{_identifier(schema, 'schema')}`"
        )
        cursor.execute(sql)
    finally:
        cursor.close()
    return f"{catalog}.{schema}.{load['table_name']}"
