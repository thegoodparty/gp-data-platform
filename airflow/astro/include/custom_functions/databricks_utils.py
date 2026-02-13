import logging
from typing import List

from databricks import sql as databricks_sql
from databricks.sql.client import Connection

logger = logging.getLogger("airflow.task")


def get_databricks_connection(
    host: str,
    http_path: str,
    access_token: str,
) -> Connection:
    """
    Create a connection to Databricks using the SQL connector.
    """
    try:
        connection = databricks_sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=access_token,
        )
        logger.info("Databricks connection established successfully")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to Databricks: {e}")
        raise


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
