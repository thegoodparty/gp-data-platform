import logging
import time
from typing import List

from databricks import sql as databricks_sql
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql.client import Connection

logger = logging.getLogger("airflow.task")


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
    total_deleted = 0
    full_table_name = f"`{catalog}`.`{schema}`.`{table}`"

    for i in range(0, len(values), batch_size):
        batch = values[i : i + batch_size]
        # Build a parameterized-style IN clause with quoted values
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
        cursor.execute(
            f"SELECT 1 FROM information_schema.tables "
            f"WHERE table_catalog = '{catalog}' "
            f"AND table_schema = '{schema}' "
            f"AND table_name = 'l2_expired_voters'"
        )
        if not cursor.fetchone():
            logger.info("Staging table does not exist yet — no files processed.")
            return set()

        cursor.execute(
            f"SELECT DISTINCT trim(file_name) AS file_name "
            f"FROM (SELECT explode(split(source_files, ', ')) AS file_name "
            f"FROM {full_table_name})"
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
    batch_size: int = 1000,
) -> int:
    """
    Write expired voter IDs to a staging table in Databricks for auditability.

    Creates the schema and table if they don't exist.
    Appends new records each DAG run to maintain a full history.

    Returns the number of rows inserted.
    """
    full_table_name = f"`{catalog}`.`{schema}`.`l2_expired_voters`"
    source_files_str = ", ".join(source_files).replace("'", "\\'")
    dag_run_id_safe = dag_run_id.replace("'", "\\'")

    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {full_table_name} ("
            "  lalvoterid STRING,"
            "  source_files STRING,"
            "  ingested_at TIMESTAMP,"
            "  dag_run_id STRING"
            ")"
        )

        total_inserted = 0
        for i in range(0, len(lalvoterids), batch_size):
            batch = lalvoterids[i : i + batch_size]
            values = ", ".join(
                f"('{vid}', '{source_files_str}', current_timestamp(), '{dag_run_id_safe}')"
                for vid in batch
            )
            cursor.execute(f"INSERT INTO {full_table_name} VALUES {values}")
            total_inserted += len(batch)
            logger.info(
                f"Staged batch {i // batch_size + 1} "
                f"({len(batch)} rows, {total_inserted} of {len(lalvoterids)})"
            )

        logger.info(f"Total staged to {full_table_name}: {total_inserted} rows")
        return total_inserted
    finally:
        cursor.close()
