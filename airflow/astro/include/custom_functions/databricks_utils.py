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


def delete_from_databricks_table(
    connection: Connection,
    catalog: str,
    schema: str,
    table: str,
    column: str,
    values: List[str],
    batch_size: int = 1000,
) -> int:
    """
    Delete rows from a Databricks Delta table where column matches any of the given values.

    Processes in batches to avoid query size limits.

    Returns the total number of rows deleted.
    """
    _validate_lalvoterids(values)
    total_deleted = 0
    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        placeholders = ", ".join(f"'{v}'" for v in batch)
        query = f"DELETE FROM {full_table_name} WHERE `{column}` IN ({placeholders})"

        logger.info(
            f"Deleting batch {i // batch_size + 1} "
            f"({len(batch)} values, {i + 1}-{i + len(batch)} of {len(values)}) "
            f"from {full_table_name}"
        )

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            deleted = cursor.rowcount if cursor.rowcount >= 0 else 0
            total_deleted += deleted
            logger.info(f"  Deleted {deleted} rows")
        finally:
            cursor.close()

    logger.info(f"Total deleted from {full_table_name}: {total_deleted} rows")
    return total_deleted


def count_in_databricks_table(
    connection: Connection,
    catalog: str,
    schema: str,
    table: str,
    column: str,
    values: List[str],
    batch_size: int = 1000,
) -> int:
    """
    Count rows in a Databricks Delta table where column matches any of the given values.

    Processes in batches to avoid query size limits.

    Returns the total number of matching rows.
    """
    _validate_lalvoterids(values)
    total_count = 0
    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        placeholders = ", ".join(f"'{v}'" for v in batch)
        query = (
            f"SELECT COUNT(*) FROM {full_table_name} "
            f"WHERE `{column}` IN ({placeholders})"
        )

        cursor = connection.cursor()
        try:
            cursor.execute(query)
            row = cursor.fetchone()
            total_count += row[0] if row else 0
        finally:
            cursor.close()

    logger.info(f"Count of matching rows in {full_table_name}: {total_count}")
    return total_count


def get_processed_files(
    connection: Connection,
    catalog: str,
    schema: str,
) -> set:
    """
    Query the l2_expired_voters staging table for file names that have
    already been processed.  Returns a set of individual file name strings.
    """
    full_table_name = f"`{catalog}`.`{schema}`.`l2_expired_voters`"
    cursor = connection.cursor()
    try:
        # Check if table exists first
        catalog_safe = catalog.replace("\\", "\\\\").replace("'", "\\'")
        schema_safe = schema.replace("\\", "\\\\").replace("'", "\\'")
        cursor.execute(
            f"SELECT 1 FROM information_schema.tables "
            f"WHERE table_catalog = '{catalog_safe}' "
            f"AND table_schema = '{schema_safe}' "
            f"AND table_name = 'l2_expired_voters'"
        )
        if not cursor.fetchone():
            logger.info("Staging table does not exist yet — no files processed.")
            return set()

        cursor.execute(
            f"SELECT DISTINCT trim(file_name) AS file_name "
            f"FROM (SELECT explode(split(source_files, ', ')) AS file_name "
            f"FROM {full_table_name} WHERE status = 'completed')"
        )
        processed = {row[0] for row in cursor.fetchall()}
        logger.info(f"Already-processed files: {processed}")
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
    batch_size: int = 1000,
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
    source_files_str = ", ".join(source_files).replace("\\", "\\\\").replace("'", "\\'")
    dag_run_id_safe = dag_run_id.replace("\\", "\\\\").replace("'", "\\'")

    # Use the most recent SFTP file timestamp (or NULL if unavailable)
    file_ts = file_timestamps or {}
    latest_ts = max(file_ts.values()) if file_ts else None
    ts_sql = f"'{latest_ts}'" if latest_ts else "NULL"

    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {full_table_name} ("
            "  lalvoterid STRING,"
            "  source_files STRING,"
            "  file_modified_at TIMESTAMP,"
            "  ingested_at TIMESTAMP,"
            "  dag_run_id STRING,"
            "  status STRING"
            ")"
        )

        # Clean up any previous pending rows for this dag_run_id (idempotent retry)
        cursor.execute(
            f"DELETE FROM {full_table_name} "
            f"WHERE dag_run_id = '{dag_run_id_safe}' AND status = 'pending'"
        )

        total_inserted = 0
        for i in range(0, len(lalvoterids), batch_size):
            batch = lalvoterids[i : i + batch_size]
            values = ", ".join(
                f"('{vid}', '{source_files_str}', {ts_sql}, "
                f"current_timestamp(), '{dag_run_id_safe}', 'pending')"
                for vid in batch
            )
            cursor.execute(
                f"INSERT INTO {full_table_name} "
                f"(lalvoterid, source_files, file_modified_at, ingested_at, "
                f"dag_run_id, status) "
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


def get_staged_voter_ids(
    connection: Connection,
    catalog: str,
    schema: str,
    dag_run_id: str,
) -> List[str]:
    """
    Read staged LALVOTERIDs for a specific DAG run from the staging table.

    Returns a list of LALVOTERID strings.
    """
    full_table_name = f"`{catalog}`.`{schema}`.`l2_expired_voters`"
    dag_run_id_safe = dag_run_id.replace("\\", "\\\\").replace("'", "\\'")

    cursor = connection.cursor()
    try:
        cursor.execute(
            f"SELECT lalvoterid FROM {full_table_name} "
            f"WHERE dag_run_id = '{dag_run_id_safe}'"
        )
        ids = [row[0] for row in cursor.fetchall()]
        logger.info(f"Read {len(ids)} staged LALVOTERIDs for dag_run_id={dag_run_id}")
        return ids
    finally:
        cursor.close()


def mark_staging_complete(
    connection: Connection,
    catalog: str,
    schema: str,
    dag_run_id: str,
) -> int:
    """
    Update staged rows from 'pending' to 'completed' for a DAG run.

    Returns the number of rows updated.
    """
    full_table_name = f"`{catalog}`.`{schema}`.`l2_expired_voters`"
    dag_run_id_safe = dag_run_id.replace("\\", "\\\\").replace("'", "\\'")

    cursor = connection.cursor()
    try:
        cursor.execute(
            f"UPDATE {full_table_name} "
            f"SET status = 'completed' "
            f"WHERE dag_run_id = '{dag_run_id_safe}' AND status = 'pending'"
        )
        updated = cursor.rowcount if cursor.rowcount >= 0 else 0
        logger.info(f"Marked {updated} staged rows as completed for {dag_run_id}")
        return updated
    finally:
        cursor.close()
