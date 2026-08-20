"""SFTP, S3 and Databricks handling for the load_l2_voter_files DAG.

Nothing is recorded between runs: the SFTP listing against S3 decides what to copy, and S3
against each table's last-altered time decides what to load. Staged files live at
`{prefix}/{folder}/{file}`, where folder is the state code, or EXPIRED_FOLDER for the
expired-ID file.
"""

import io
import logging
import os
import re
import shutil
from collections.abc import Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from zipfile import ZipFile

from boto3.s3.transfer import TransferConfig
from include.custom_functions.databricks_utils import execute_with_retry
from paramiko import SFTPClient, Transport
from paramiko.sftp_attr import SFTPAttributes

logger = logging.getLogger("airflow.task")

EXPIRED_FOLDER = "EXPIRED"
EXPIRED_TABLE = "l2_s3_expired_voters"
EXPIRED_DIR = "/Reference/ToBeDeleted"
_EXPIRED_PATTERN = re.compile(r"^Manual_ID_Omits\.tab$")

# One entry per family of files we mirror, stating that family's grammar once. An archive is named
# `stem` + `archive_suffix`; each member we keep is `stem` + a `members` key, and that key's value
# is both the member's type and its table's suffix. The patterns below all derive from this.
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

# L2 pads its data dictionaries with a preamble and a trailing legend. A member type absent here
# is staged verbatim.
_DICTIONARY_HEADER_ROWS = 15
_DICTIONARY_FOOTER_ROWS = {
    "demographic_data_dictionary": 24,
    "uniform_data_dictionary": 24,
    "vote_history_data_dictionary": 4,
}

# 16 MiB parts stay under S3's 10,000-part ceiling for the largest members.
_TRANSFER_CONFIG = TransferConfig(multipart_chunksize=16 * 1024 * 1024, max_concurrency=4)
# paramiko reads 32 KiB per request, so an unprefetched download is round-trip bound.
_PREFETCH_REQUESTS = 64


class _SequentialReader:
    """Exposes only read(), so boto3 streams a zip member instead of seeking it.

    ZipExtFile fakes seek by re-decompressing from the start of the member, which boto3's seekable
    upload path would trigger once per part.
    """

    def __init__(self, handle):
        self._handle = handle

    def read(self, size=-1) -> bytes:
        return self._handle.read(size)


@contextmanager
def sftp_session(host: str, port: int, username: str, password: str) -> Iterator[SFTPClient]:
    """An SFTP client with keep-alive, torn down on exit. Task retries cover a failed connect."""
    transport = Transport((host, port))
    transport.set_keepalive(30)
    try:
        transport.connect(username=username, password=password)
        sftp_client = SFTPClient.from_transport(transport)
        if sftp_client is None:
            raise ValueError(f"Could not open an SFTP session on {host}")
        yield sftp_client
    finally:
        transport.close()


def _list_matching(
    sftp_client: SFTPClient, remote_dir: str, pattern: re.Pattern
) -> list[tuple[SFTPAttributes, re.Match[str]]]:
    """The files in remote_dir whose names match pattern, each with its match."""
    matches = ((a, pattern.match(a.filename)) for a in sftp_client.listdir_attr(remote_dir))
    return [(attributes, match) for attributes, match in matches if match is not None]


def source_file_type(source_file_name: str) -> str | None:
    """Classify a staged file, or None. Doubles as the destination table's suffix."""
    for pattern, file_type in _MEMBER_PATTERNS:
        if pattern.match(source_file_name):
            return file_type
    return None


def _source(group: str, remote_dir: str, attributes: SFTPAttributes, folder: str, members: list[str]) -> dict:
    return {
        "group": group,
        "folder": folder,
        "remote_path": f"{remote_dir}/{attributes.filename}",
        "members": members,
        # A server that omits the size costs us the disk precheck, not the transfer. Without an
        # mtime there is no way to tell a stale staged copy from a current one, so we always recopy.
        "size_bytes": attributes.st_size or 0,
        "modified_at": (
            datetime.fromtimestamp(attributes.st_mtime, tz=UTC).isoformat() if attributes.st_mtime else None
        ),
    }


def list_remote_sources(sftp_client: SFTPClient) -> list[dict]:
    """Every file we mirror: the newest publication per state of each group, plus the expired file.

    A state L2 has pulled while rebuilding is simply absent, not an error.
    """
    sources: list[dict] = []
    for group, spec in SOURCE_GROUPS.items():
        # L2 leaves superseded publications in place and only the newest ever becomes a table, so
        # copying an older one would be wasted bandwidth.
        newest: dict[str, tuple[str, dict]] = {}
        for attributes, match in _list_matching(sftp_client, spec["remote_dir"], _ARCHIVE_PATTERNS[group]):
            folder, version = match["folder"].upper(), match["version"]
            if folder in newest and version <= newest[folder][0]:
                continue
            stem = attributes.filename.removesuffix(spec["archive_suffix"])
            members = [stem + suffix for suffix in spec["members"]]
            newest[folder] = (version, _source(group, spec["remote_dir"], attributes, folder, members))
        logger.info(f"{group}: {len(newest)} archive(s) in {spec['remote_dir']}")
        sources.extend(source for _, source in newest.values())

    # The expired feed is auxiliary, so losing its directory must not block the archive groups.
    try:
        expired = _list_matching(sftp_client, EXPIRED_DIR, _EXPIRED_PATTERN)
    except FileNotFoundError:
        logger.error(f"Expired-voter directory not found, skipping it: {EXPIRED_DIR}")
        expired = []
    sources.extend(
        _source(EXPIRED_FOLDER, EXPIRED_DIR, attributes, EXPIRED_FOLDER, [attributes.filename])
        for attributes, _ in expired
    )

    return sorted(sources, key=lambda source: source["remote_path"])


def list_s3_objects(s3_client, bucket: str, prefix: str) -> dict[str, datetime]:
    """Every staged object's last-modified time, keyed by `{folder}/{file name}`."""
    staged: dict[str, datetime] = {}
    for page in s3_client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            staged[obj["Key"].removeprefix(f"{prefix}/")] = obj["LastModified"]
    return staged


def plan_transfers(sources: list[dict], staged: dict[str, datetime]) -> list[dict]:
    """The sources S3 is missing, holds only part of, or holds an older copy of.

    Requiring every member, not just one, retries a run that died mid-archive.
    """
    pending = []
    for source in sources:
        modified_at = source["modified_at"] and datetime.fromisoformat(source["modified_at"])
        staged_at = [staged.get(f"{source['folder']}/{member}") for member in source["members"]]
        if modified_at and all(at is not None and at >= modified_at for at in staged_at):
            continue
        pending.append(source)
    return pending


def _trim_data_dictionary(raw: bytes, footer_rows: int) -> bytes:
    # Imported here so the scheduler does not pay for pandas on every DAG parse.
    import pandas as pd

    frame = pd.read_csv(
        io.BytesIO(raw), skiprows=_DICTIONARY_HEADER_ROWS, skipfooter=footer_rows, engine="python"
    )
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False)
    return buffer.getvalue().encode()


def _upload(s3_client, bucket: str, folder_prefix: str, member: str, handle) -> None:
    key = f"{folder_prefix}{member}"
    logger.info(f"Uploading s3://{bucket}/{key}")
    footer_rows = _DICTIONARY_FOOTER_ROWS.get(source_file_type(member) or "")
    if footer_rows is None:
        s3_client.upload_fileobj(handle, bucket, key, Config=_TRANSFER_CONFIG)
    else:
        s3_client.put_object(Bucket=bucket, Key=key, Body=_trim_data_dictionary(handle.read(), footer_rows))


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

    free_bytes = shutil.disk_usage(staging_dir).free
    if size_bytes and free_bytes < size_bytes * 1.1:
        raise ValueError(
            f"{file_name} needs {size_bytes / 1024**3:.1f} GB but only "
            f"{free_bytes / 1024**3:.1f} GB is free on {staging_dir}"
        )

    logger.info(f"Downloading {source['remote_path']} ({size_bytes / 1024**3:.2f} GB)")
    sftp_client.get(source["remote_path"], local_path, max_concurrent_prefetch_requests=_PREFETCH_REQUESTS)
    try:
        if not file_name.lower().endswith(".zip"):
            # A plain file is seekable, so boto3 uploads its parts concurrently.
            with open(local_path, "rb") as handle:
                _upload(s3_client, bucket, folder_prefix, file_name, handle)
            return [f"{folder_prefix}{file_name}"]

        with ZipFile(local_path) as zip_file:
            missing = [member for member in members if member not in zip_file.namelist()]
            if missing:
                raise ValueError(f"{file_name} did not contain {missing}")
            for member in members:
                with zip_file.open(member) as handle:
                    _upload(s3_client, bucket, folder_prefix, member, _SequentialReader(handle))
            return [f"{folder_prefix}{member}" for member in members]
    finally:
        os.remove(local_path)


def plan_loads(staged: dict[str, datetime], loaded_at: dict[str, datetime]) -> list[dict]:
    """The newest staged file per table, minus the tables already built from it.

    S3 keeps every dated snapshot, so only the newest is a candidate.
    """
    latest: dict[str, tuple[str, str, datetime]] = {}
    unclassified: list[str] = []
    for relative_key, staged_at in staged.items():
        folder, _, file_name = relative_key.partition("/")
        if folder == EXPIRED_FOLDER:
            table_name = EXPIRED_TABLE
        elif _STATE_FOLDER.match(folder) and (file_type := source_file_type(file_name)):
            table_name = f"l2_s3_{folder.lower()}_{file_type}"
        else:
            # Staged but unclassifiable means it was uploaded and will never load, silently.
            unclassified.append(relative_key)
            continue
        if table_name not in latest or staged_at > latest[table_name][2]:
            latest[table_name] = (folder, file_name, staged_at)

    if unclassified:
        logger.warning(f"{len(unclassified)} staged file(s) match no group: {sorted(unclassified)[:10]}")

    return [
        {"folder": folder, "source_file_name": file_name, "table_name": table_name}
        for table_name, (folder, file_name, staged_at) in sorted(latest.items())
        if loaded_at.get(table_name) is None or loaded_at[table_name] < staged_at
    ]


def create_schema(connection, catalog: str, schema: str) -> None:
    with closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")


def table_loaded_at(connection, catalog: str, schema: str) -> dict[str, datetime]:
    """When each table in the target schema was last rebuilt."""
    with closing(connection.cursor()) as cursor:
        execute_with_retry(
            cursor,
            f"SELECT table_name, last_altered FROM `{catalog}`.information_schema.tables "
            "WHERE table_schema = :schema",
            parameters={"schema": schema},
        )
        # The connector returns TIMESTAMP as a naive datetime and last_altered is UTC. Without the
        # tag, comparing it against S3's aware LastModified raises TypeError.
        return {name: at.replace(tzinfo=UTC) for name, at in cursor.fetchall() if at is not None}


def load_table(connection, catalog: str, schema: str, bucket: str, prefix: str, load: dict) -> str:
    """Rebuild one table from its staged file, returning its fully qualified name.

    Every column is read as a string so the zero-padded codes L2 carries (ZIP, ZipPlus4, DPBC,
    FIPS) survive. The staged path is bound as a parameter; the identifiers cannot be bound in
    DDL, and are a module constant, an Airflow variable and a name built above.
    """
    file_name = load["source_file_name"]
    sql = (
        f"CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`{load['table_name']}` "
        "CLUSTER BY AUTO "
        "AS SELECT * EXCEPT (_rescued_data), current_timestamp() AS loaded_at "
        "FROM read_files(:path, format => 'csv', delimiter => :delimiter, "
        "header => true, inferColumnTypes => false)"
    )
    parameters = {
        "path": f"s3://{bucket}/{prefix}/{load['folder']}/{file_name}",
        "delimiter": "\t" if file_name.endswith(".tab") else ",",
    }
    logger.info(f"{sql} -- {parameters}")
    with closing(connection.cursor()) as cursor:
        execute_with_retry(cursor, sql, parameters=parameters)
    return f"{catalog}.{schema}.{load['table_name']}"
