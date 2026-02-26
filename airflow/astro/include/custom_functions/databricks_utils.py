import logging
import re
import time
from typing import Dict, List

from databricks import sql as databricks_sql
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql.client import Connection

LALVOTERID_PATTERN = re.compile(r"^LAL[A-Z]{2}\d+$")

logger = logging.getLogger("airflow.task")


def _validate_lalvoterids(values: List[str]) -> None:
    """Raise ValueError if any value doesn't match the expected LALVOTERID format."""
    bad = [v for v in values if not LALVOTERID_PATTERN.match(v)]
    if bad:
        raise ValueError(
            f"{len(bad)} invalid LALVOTERID(s) — refusing to build SQL: {bad[:5]}"
        )


def get_databricks_connection(
    host: str,
    http_path: str,
    client_id: str,
    client_secret: str,
    max_retries: int = 20,
    retry_delay: int = 30,
) -> Connection:
    """
    Create a connection to Databricks using OAuth M2M (service principal) credentials.

    Retries on failure to allow for SQL warehouse cold-start (~10 min).
    """
    # Normalize — server_hostname needs bare host, Config needs https://
    hostname = host.removeprefix("https://").removeprefix("http://")

    def credential_provider():
        config = Config(
            host=f"https://{hostname}",
            client_id=client_id,
            client_secret=client_secret,
        )
        return oauth_service_principal(config)

    for attempt in range(max_retries):
        try:
            connection = databricks_sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                credentials_provider=credential_provider,
            )
            logger.info("Databricks connection established successfully")
            return connection
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(
                    f"Databricks connection failed after {max_retries} attempts: {e}"
                )
                raise
            logger.warning(
                f"Databricks connection attempt {attempt + 1}/{max_retries} failed: {e}. "
                f"Retrying in {retry_delay}s (warehouse may be starting)..."
            )
            time.sleep(retry_delay)
    raise RuntimeError("Unreachable")


def get_processed_files(
    connection: Connection,
    catalog: str,
    schema: str,
) -> set:
    """
    Query the l2_expired_voters staging table for files that have already
    been processed.  Returns a set of composite ``filename|mtime`` keys so
    that republished files with the same name but a new mtime are re-processed.
    """
    full_table_name = f"`{catalog}`.`{schema}`.`l2_expired_voters`"
    cursor = connection.cursor()
    try:
        # Check if table exists first
        catalog_safe = catalog.replace("\\", "\\\\").replace("'", "\\'")
        schema_safe = schema.replace("\\", "\\\\").replace("'", "\\'")
        cursor.execute(
            f"SELECT 1 FROM `{catalog_safe}`.information_schema.tables "
            f"WHERE table_catalog = '{catalog_safe}' "
            f"AND table_schema = '{schema_safe}' "
            f"AND table_name = 'l2_expired_voters'"
        )
        if not cursor.fetchone():
            logger.info("Staging table does not exist yet — no files processed.")
            return set()

        cursor.execute(
            f"SELECT DISTINCT trim(file_key) AS file_key "
            f"FROM (SELECT explode(split(source_file_keys, ', ')) AS file_key "
            f"FROM {full_table_name})"
        )
        processed = {row[0] for row in cursor.fetchall()}
        logger.info(f"Already-processed file keys: {processed}")
        return processed
    finally:
        cursor.close()


def stage_expired_voter_ids(
    connection: Connection,
    catalog: str,
    schema: str,
    lalvoterids: List[str],
    source_files: List[str],
    dag_run_id: str,
    file_timestamps: Dict[str, str] | None = None,
    batch_size: int = 10_000,
) -> int:
    """
    Write expired voter IDs to a staging table in Databricks for auditability.

    Creates the schema and table if they don't exist.
    Appends new records each DAG run to maintain a full history.

    Args:
        file_timestamps: Dict mapping source file basename to SFTP mtime
            (ISO 8601 string).  The most recent timestamp is stored as
            ``file_modified_at`` on every row in the batch.

    Returns the number of rows inserted.
    """
    _validate_lalvoterids(lalvoterids)
    full_table_name = f"`{catalog}`.`{schema}`.`l2_expired_voters`"
    dag_run_id_safe = dag_run_id.replace("\\", "\\\\").replace("'", "\\'")

    # Plain comma-separated file list (human-readable)
    source_files_str = ", ".join(source_files).replace("\\", "\\\\").replace("'", "\\'")

    # Composite "filename|mtime" keys for idempotency so republished
    # files with the same name but new content are re-processed.
    file_ts = file_timestamps or {}
    source_keys = [f"{f}|{file_ts.get(f, '')}" for f in source_files]
    source_file_keys_str = (
        ", ".join(source_keys).replace("\\", "\\\\").replace("'", "\\'")
    )

    latest_ts = max(file_ts.values()) if file_ts else None
    ts_sql = f"'{latest_ts}'" if latest_ts else "NULL"

    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {full_table_name} ("
            "  lalvoterid STRING,"
            "  source_files STRING,"
            "  source_file_keys STRING,"
            "  file_modified_at TIMESTAMP,"
            "  ingested_at TIMESTAMP,"
            "  dag_run_id STRING"
            ")"
        )

        # Clean up any previous rows for this dag_run_id (idempotent retry)
        cursor.execute(
            f"DELETE FROM {full_table_name} " f"WHERE dag_run_id = '{dag_run_id_safe}'"
        )

        total_inserted = 0
        for i in range(0, len(lalvoterids), batch_size):
            batch = lalvoterids[i : i + batch_size]
            values = ", ".join(
                f"('{vid}', '{source_files_str}', '{source_file_keys_str}', "
                f"{ts_sql}, current_timestamp(), '{dag_run_id_safe}')"
                for vid in batch
            )
            cursor.execute(
                f"INSERT INTO {full_table_name} "
                f"(lalvoterid, source_files, source_file_keys, file_modified_at, "
                f"ingested_at, dag_run_id) "
                f"VALUES {values}"
            )
            total_inserted += len(batch)
            logger.info(
                f"Staged batch {i // batch_size + 1} "
                f"({len(batch)} rows, {total_inserted} of {len(lalvoterids)})"
            )

        logger.info(f"Total staged to {full_table_name}: {total_inserted} rows")
        return total_inserted
    finally:
        cursor.close()
