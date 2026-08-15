"""L2 file handling for the load_l2_voter_files DAG.

Nothing is recorded between runs: the SFTP listing versus S3 decides what to copy, and S3 versus
each table's last-altered time decides what to load.

Staged files live at `{prefix}/{folder}/{file}`, where folder is the state for the voter and
Haystaq archives and EXPIRED_FOLDER for the expired-ID file.
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
from include.custom_functions.sftp_utils import download, list_matching
from paramiko import SFTPClient

logger = logging.getLogger("airflow.task")

EXPIRED_FOLDER = "EXPIRED"
EXPIRED_TABLE = "l2_s3_expired_voters"

# One entry per family of files we mirror, stating that family's grammar once. `stem` captures the
# state as `folder` and the publication as `version`; the archive is the stem plus
# `archive_suffix`, each kept member is the stem plus a `members` key, and that member's value is
# the file type, which is also the destination table's suffix. Everything below derives from this.
_L2_STEM = r"--(?P<folder>[A-Z]{2})--(?P<version>\d{4}-\d{2}-\d{2})"
# The server's two Haystaq directories hold the same files, so one covers both types.
_HAYSTAQ_DIR = "/L2-Haystaq Issue Model Scores"

SOURCE_GROUPS: dict[str, dict] = {
    "VM2": {
        "remote_dir": "/VMFiles",
        "stem": rf"VM2{_L2_STEM}",
        "archive_suffix": ".zip",
        "members": {
            "-DEMOGRAPHIC.tab": "demographic",
            "-DEMOGRAPHIC_DataDictionary.csv": "demographic_data_dictionary",
            "-VOTEHISTORY.tab": "vote_history",
            "-VOTEHISTORY_DataDictionary.csv": "vote_history_data_dictionary",
        },
    },
    "VM2Uniform": {
        "remote_dir": "/VM2Uniform",
        "stem": rf"VM2Uniform{_L2_STEM}",
        "archive_suffix": ".zip",
        "members": {".tab": "uniform", "_DataDictionary.csv": "uniform_data_dictionary"},
    },
    "HaystaqFlags": {
        "remote_dir": _HAYSTAQ_DIR,
        "stem": r"(?P<folder>[a-z]{2})_haystaqdnaflags_(?P<version>\d{8})",
        "archive_suffix": ".tab.zip",
        "members": {".tab": "haystaq_dna_flags"},
    },
    "HaystaqScores": {
        "remote_dir": _HAYSTAQ_DIR,
        "stem": r"(?P<folder>[a-z]{2})_haystaqdnascores_(?P<version>\d{8})",
        "archive_suffix": ".tab.zip",
        "members": {".tab": "haystaq_dna_scores"},
    },
}
_ARCHIVE_PATTERNS = {
    name: re.compile(rf"^{spec['stem']}{re.escape(spec['archive_suffix'])}$")
    for name, spec in SOURCE_GROUPS.items()
}
_MEMBER_PATTERNS: tuple[tuple[re.Pattern, str], ...] = tuple(
    (re.compile(rf"^{spec['stem']}{re.escape(suffix)}$"), file_type)
    for spec in SOURCE_GROUPS.values()
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
    """Classify a staged file, or None. Doubles as the destination table suffix."""
    for pattern, file_type in _MEMBER_PATTERNS:
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


def _source(group: str, remote_dir: str, attributes, folder: str, members: list[str] | None) -> dict:
    return {
        "group": group,
        "folder": folder,
        "remote_path": f"{remote_dir}/{attributes.filename}",
        "members": members,
        # An SFTP server that omits the size costs us the disk precheck, not the transfer.
        "size_bytes": attributes.st_size or 0,
        # No mtime means no way to tell a stale copy from a current one, so the source is always
        # re-transferred. Defaulting to the epoch instead would mark it permanently up to date.
        "modified_at": (
            datetime.fromtimestamp(attributes.st_mtime, tz=UTC).isoformat() if attributes.st_mtime else None
        ),
    }


def list_remote_sources(sftp_client: SFTPClient, expired_dir: str, expired_pattern: str) -> list[dict]:
    """Every file we mirror: the newest per state of each group, plus the expired-ID file.

    A state L2 has pulled while rebuilding is simply absent, not an error.
    """
    sources: list[dict] = []
    for name, spec in SOURCE_GROUPS.items():
        newest: dict[str, tuple[str, dict]] = {}
        for attributes, match in list_matching(sftp_client, spec["remote_dir"], _ARCHIVE_PATTERNS[name]):
            folder = match["folder"].upper()
            # Only the newest publication per state becomes a table, so transferring a superseded
            # one the server still lists would be wasted bandwidth.
            if folder in newest and match["version"] <= newest[folder][0]:
                continue
            stem = attributes.filename.removesuffix(spec["archive_suffix"])
            newest[folder] = (
                match["version"],
                _source(
                    group=name,
                    remote_dir=spec["remote_dir"],
                    attributes=attributes,
                    folder=folder,
                    members=[stem + suffix for suffix in spec["members"]],
                ),
            )
        sources.extend(source for _, source in newest.values())

    # Both the directory and the pattern are operator-configured, so neither should be able to
    # block the archive groups. Their directories are fixed, and their absence stays a real alarm.
    try:
        expired = list_matching(sftp_client, expired_dir, re.compile(expired_pattern))
    except FileNotFoundError:
        logger.error(f"Expired-voter directory not found, skipping it: {expired_dir}")
        expired = []
    except re.error as exc:
        logger.error(f"Invalid expired-voter pattern, skipping it: {expired_pattern!r} ({exc})")
        expired = []

    for attributes, _ in expired:
        # A plain file stages under its own name; a zip's contents are only knowable once opened.
        members = None if attributes.filename.lower().endswith(".zip") else [attributes.filename]
        sources.append(
            _source(
                group=EXPIRED_FOLDER,
                remote_dir=expired_dir,
                attributes=attributes,
                folder=EXPIRED_FOLDER,
                members=members,
            )
        )

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
        # Members we cannot name, or no server timestamp to compare against: either way there is
        # nothing to diff, so re-transfer rather than assume the staged copy is current.
        if source["members"] is None or source["modified_at"] is None:
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
    """Download one source file and upload it, or the members we keep, into `{prefix}/{folder}/`.

    Only the compressed archive touches disk; its members stream out of the zip into S3.
    """
    folder_prefix = f"{prefix}/{source['folder']}/"
    file_name = source["remote_path"].rsplit("/", 1)[-1]
    local_path = os.path.join(staging_dir, file_name)
    members = source["members"]
    size_bytes = source["size_bytes"]

    # Refused before the download, so the daily retry costs nothing.
    if members is None:
        raise ValueError(
            f"{file_name} is an archive whose members cannot be derived from its name. "
            "Only plain files are supported here; unpack it upstream or add it to "
            "SOURCE_GROUPS with its member suffixes."
        )

    free_bytes = shutil.disk_usage(staging_dir).free
    if not size_bytes:
        logger.warning(f"{file_name} has no size on the server; skipping the disk-space check")
    elif free_bytes < size_bytes * 1.1:
        raise ValueError(
            f"{file_name} needs {size_bytes / 1024**3:.1f} GB but only "
            f"{free_bytes / 1024**3:.1f} GB is free on {staging_dir}"
        )

    logger.info(f"Downloading {source['remote_path']} ({size_bytes / 1024**3:.2f} GB)")
    download(sftp_client, source["remote_path"], local_path)
    try:
        if not file_name.lower().endswith(".zip"):
            key = f"{folder_prefix}{file_name}"
            with open(local_path, "rb") as handle:
                _upload(s3_client, bucket, key, handle, file_name)
            return [key]

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

    Identifiers cannot be bound in DDL, so they are validated instead. Every column is read as a
    string to keep the zero-padded codes L2 carries (ZIP, ZipPlus4, DPBC, FIPS) intact.
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
