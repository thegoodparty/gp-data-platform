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
from include.custom_functions.databricks_utils import execute_with_retry
from include.custom_functions.l2_sftp import download, list_matching
from paramiko import SFTPClient

logger = logging.getLogger("airflow.task")

EXPIRED_FOLDER = "EXPIRED"
EXPIRED_TABLE = "l2_s3_expired_voters"

# The per-state archives, keyed by the prefix L2 names them with. Each member suffix maps to the
# file type that becomes the destination table's suffix, so the archive grammar is stated once:
# the archive patterns and the staged-file patterns below are both derived from it.
_STEM = r"--(?P<folder>[A-Z]{2})--\d{4}-\d{2}-\d{2}"
ARCHIVE_GROUPS: dict[str, dict] = {
    "VM2Uniform": {
        "remote_dir": "/VM2Uniform",
        "members": {".tab": "uniform", "_DataDictionary.csv": "uniform_data_dictionary"},
    },
    "VM2": {
        "remote_dir": "/VMFiles",
        "members": {
            "-DEMOGRAPHIC.tab": "demographic",
            "-DEMOGRAPHIC_DataDictionary.csv": "demographic_data_dictionary",
            "-VOTEHISTORY.tab": "vote_history",
            "-VOTEHISTORY_DataDictionary.csv": "vote_history_data_dictionary",
        },
    },
}
_ARCHIVE_PATTERNS = {name: re.compile(rf"^{name}{_STEM}\.zip$") for name in ARCHIVE_GROUPS}
_SOURCE_FILE_TYPES: tuple[tuple[re.Pattern, str], ...] = tuple(
    (re.compile(rf"^{name}{_STEM}{re.escape(suffix)}$"), file_type)
    for name, spec in ARCHIVE_GROUPS.items()
    for suffix, file_type in spec["members"].items()
)
_STATE_FOLDER = re.compile(r"^[A-Z]{2}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")

# L2 pads its data dictionaries with a preamble and a trailing legend; VOTEHISTORY's is shorter.
_DICTIONARY_HEADER_ROWS = 15
_VOTEHISTORY_FOOTER_ROWS = 4
_DEFAULT_FOOTER_ROWS = 24

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
    footer_rows = _VOTEHISTORY_FOOTER_ROWS if "VOTEHISTORY" in member else _DEFAULT_FOOTER_ROWS
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
        "members": members,
        # An SFTP server that omits the size costs us the disk precheck, not the transfer.
        "size_bytes": attributes.st_size or 0,
        "modified_at": datetime.fromtimestamp(attributes.st_mtime or 0, tz=UTC).isoformat(),
    }


def list_remote_sources(sftp_client: SFTPClient, expired_dir: str, expired_pattern: str) -> list[dict]:
    """Every L2 file we mirror: the per-state archives plus the expired-ID file.

    A state L2 has pulled while rebuilding is simply absent, not an error.
    """
    sources: list[dict] = []
    for name, spec in ARCHIVE_GROUPS.items():
        for attributes, match in list_matching(sftp_client, spec["remote_dir"], _ARCHIVE_PATTERNS[name]):
            base = attributes.filename.removesuffix(".zip")
            sources.append(
                _source(
                    remote_dir=spec["remote_dir"],
                    attributes=attributes,
                    folder=match["folder"],
                    members=[base + suffix for suffix in spec["members"]],
                )
            )

    for attributes, _ in list_matching(sftp_client, expired_dir, re.compile(expired_pattern)):
        # A plain file stages under its own name; a zip's contents are only knowable once opened.
        members = None if attributes.filename.lower().endswith(".zip") else [attributes.filename]
        sources.append(_source(expired_dir, attributes, EXPIRED_FOLDER, members))

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

    Requiring every member, not just one, retries a run that died mid-archive.
    """
    pending = []
    for source in sources:
        if source["members"] is None:
            # An archive whose members we cannot name without opening it. Re-syncing beats
            # guessing from the folder, where an unrelated object would mask a new file.
            pending.append(source)
            continue
        modified_at = datetime.fromisoformat(source["modified_at"])
        staged_at = [staged.get(f"{source['folder']}/{member}") for member in source["members"]]
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
    file_name = source["remote_path"].rsplit("/", 1)[-1]
    local_path = os.path.join(staging_dir, file_name)

    # Astro workers have a fixed 10 GiB of ephemeral storage, so a growing archive will one day
    # outgrow it. Fail with the numbers rather than filling the disk mid-download.
    free_bytes = shutil.disk_usage(staging_dir).free
    if free_bytes < source["size_bytes"] * 1.1:
        raise ValueError(
            f"{file_name} needs {source['size_bytes'] / 1024**3:.1f} GB but only "
            f"{free_bytes / 1024**3:.1f} GB is free on {staging_dir}"
        )

    logger.info(f"Downloading {source['remote_path']} ({source['size_bytes'] / 1024**3:.2f} GB)")
    download(sftp_client, source["remote_path"], local_path)
    try:
        if not file_name.lower().endswith(".zip"):
            key = f"{folder_prefix}{file_name}"
            with open(local_path, "rb") as handle:
                _upload(s3_client, bucket, key, handle, file_name)
            return [key]

        members = source["members"]
        if members is None:
            # Every staged file becomes exactly one table, so a source that could expand to
            # several would silently load only one of them. Fail this source alone: the state
            # archives keep syncing, and the Databricks step still runs off S3.
            raise ValueError(
                f"{file_name} is an archive whose members cannot be derived from its name. "
                "Only plain files are supported here; unpack it upstream or add it to "
                "ARCHIVE_GROUPS with its member suffixes."
            )

        with ZipFile(local_path) as zip_file:
            contents = [name for name in zip_file.namelist() if not name.endswith("/")]
            missing = [member for member in members if member not in contents]
            if missing:
                raise ValueError(f"{file_name} did not contain {missing}")

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


def build_load_statement(
    catalog: str, schema: str, bucket: str, prefix: str, load: dict
) -> tuple[str, dict[str, str]]:
    """The CTAS that rebuilds one table from its staged file, and its bound parameters.

    The S3 path and delimiter are bound rather than interpolated. Identifiers cannot be bound in
    DDL, but they are ours: the catalog and schema are config, and the table name is built from a
    validated folder and a fixed type.

    Every column is read as a string: the raw files carry zero-padded codes (ZIPs, ZipPlus4, DPBC)
    that type inference coerces to int, dropping leading zeros before any model can see them.
    """
    table = (
        f"`{_identifier(catalog, 'catalog')}`"
        f".`{_identifier(schema, 'schema')}`"
        f".`{_identifier(load['table_name'], 'table name')}`"
    )
    file_name = load["source_file_name"]
    sql = (
        f"CREATE OR REPLACE TABLE {table} "
        "CLUSTER BY AUTO "
        "AS SELECT * EXCEPT (_rescued_data), current_timestamp() AS loaded_at "
        "FROM read_files(:path, format => 'csv', delimiter => :delimiter, "
        "header => true, inferColumnTypes => false)"
    )
    parameters = {
        "path": f"s3://{bucket}/{prefix}/{load['folder']}/{file_name}",
        "delimiter": "\t" if file_name.endswith(".tab") else ",",
    }
    return sql, parameters


def get_table_loaded_at(connection, catalog: str, schema: str) -> dict[str, datetime]:
    """When each table in the target schema was last rebuilt."""
    cursor = connection.cursor()
    try:
        execute_with_retry(
            cursor,
            f"SELECT table_name, last_altered FROM `{_identifier(catalog, 'catalog')}`"
            ".information_schema.tables WHERE table_schema = :schema",
            parameters={"schema": schema},
        )
        # The connector returns TIMESTAMP as a naive datetime; last_altered is UTC. Without this
        # the comparison against S3's aware LastModified raises TypeError.
        return {row[0]: row[1].replace(tzinfo=UTC) for row in cursor.fetchall() if row[1] is not None}
    finally:
        cursor.close()


def create_schema(connection, catalog: str, schema: str) -> None:
    cursor = connection.cursor()
    try:
        execute_with_retry(
            cursor,
            f"CREATE SCHEMA IF NOT EXISTS `{_identifier(catalog, 'catalog')}`"
            f".`{_identifier(schema, 'schema')}`",
        )
    finally:
        cursor.close()


def load_table(connection, catalog: str, schema: str, bucket: str, prefix: str, load: dict) -> str:
    """Rebuild one table from its staged file, returning its fully qualified name."""
    sql, parameters = build_load_statement(catalog, schema, bucket, prefix, load)
    logger.info(f"{sql} -- {parameters}")
    cursor = connection.cursor()
    try:
        execute_with_retry(cursor, sql, parameters=parameters)
    finally:
        cursor.close()
    return f"{catalog}.{schema}.{load['table_name']}"
