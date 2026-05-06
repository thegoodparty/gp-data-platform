import logging
import re
import time
from typing import Dict, Generator, List, Tuple

from databricks import sql as databricks_sql
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql.client import Connection

from airflow.sdk import BaseHook, Variable

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
    use_cloud_fetch: bool = True,
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
                use_cloud_fetch=use_cloud_fetch,
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
    Query the ``l2_expired_voters_loads`` metadata table for files that have
    been fully loaded.  Returns a set of composite ``filename|mtime`` keys so
    that republished files with the same name but a new mtime are re-processed.

    Only files with a record in the loads table are considered processed —
    partial loads from failed runs are excluded because the loads record is
    written only after all batch inserts succeed.
    """
    loads_table = f"`{catalog}`.`{schema}`.`l2_expired_voters_loads`"
    cursor = connection.cursor()
    try:
        catalog_safe = catalog.replace("\\", "\\\\").replace("'", "\\'")
        schema_safe = schema.replace("\\", "\\\\").replace("'", "\\'")
        cursor.execute(
            f"SELECT 1 FROM `{catalog}`.information_schema.tables "
            f"WHERE table_catalog = '{catalog_safe}' "
            f"AND table_schema = '{schema_safe}' "
            f"AND table_name = 'l2_expired_voters_loads'"
        )
        if not cursor.fetchone():
            logger.info("Loads table does not exist yet — no files processed.")
            return set()

        cursor.execute(f"SELECT source_file_keys FROM {loads_table}")
        processed = set()
        for (keys_str,) in cursor.fetchall():
            if keys_str:
                for key in keys_str.split(", "):
                    processed.add(key.strip())
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
    batch_size: int = 10_000,  # 100k caused OOM on Astro worker; keep MERGE statements small
) -> int:
    """
    Upsert expired voter IDs into a staging table in Databricks using MERGE
    to prevent duplicates if metadata is corrupted or files are re-processed.

    Creates the schema, data table, and ``l2_expired_voters_loads`` metadata
    table if they don't exist.  A completion record is written to the loads
    table only after **all** batch merges succeed, so partial failures are
    automatically retried on the next DAG run.

    Args:
        file_timestamps: Dict mapping source file basename to SFTP mtime
            (ISO 8601 string).  The most recent timestamp is stored as
            ``file_modified_at`` on every row in the batch.

    Returns the number of rows upserted.
    """
    _validate_lalvoterids(lalvoterids)
    full_table_name = f"`{catalog}`.`{schema}`.`l2_expired_voters`"
    loads_table = f"`{catalog}`.`{schema}`.`l2_expired_voters_loads`"
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

    merge_cols = (
        "lalvoterid, source_files, source_file_keys, "
        "file_modified_at, ingested_at, dag_run_id"
    )

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
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {loads_table} ("
            "  dag_run_id STRING,"
            "  source_file_keys STRING,"
            "  row_count INT,"
            "  loaded_at TIMESTAMP"
            ")"
        )

        # Clean up any previous load record for this dag_run_id (idempotent retry)
        cursor.execute(
            f"DELETE FROM {loads_table} WHERE dag_run_id = '{dag_run_id_safe}'"
        )

        total_upserted = 0
        for i in range(0, len(lalvoterids), batch_size):
            batch = lalvoterids[i : i + batch_size]
            values = ", ".join(
                f"('{vid}', '{source_files_str}', '{source_file_keys_str}', "
                f"{ts_sql}, current_timestamp(), '{dag_run_id_safe}')"
                for vid in batch
            )
            cursor.execute(
                f"MERGE INTO {full_table_name} AS target "
                f"USING (SELECT * FROM VALUES {values} "
                f"AS t({merge_cols})) AS source "
                f"ON target.lalvoterid = source.lalvoterid "
                f"WHEN MATCHED THEN UPDATE SET "
                f"  source_files = source.source_files, "
                f"  source_file_keys = source.source_file_keys, "
                f"  file_modified_at = source.file_modified_at, "
                f"  ingested_at = source.ingested_at, "
                f"  dag_run_id = source.dag_run_id "
                f"WHEN NOT MATCHED THEN INSERT ({merge_cols}) VALUES ("
                f"  source.lalvoterid, source.source_files, "
                f"  source.source_file_keys, source.file_modified_at, "
                f"  source.ingested_at, source.dag_run_id)"
            )
            total_upserted += len(batch)
            logger.info(
                f"Merged batch {i // batch_size + 1} "
                f"({len(batch)} rows, {total_upserted} of {len(lalvoterids)})"
            )

        # Record successful load — written only after all batches succeed so
        # get_processed_files() won't mark partial failures as complete.
        cursor.execute(
            f"INSERT INTO {loads_table} "
            f"(dag_run_id, source_file_keys, row_count, loaded_at) "
            f"VALUES ('{dag_run_id_safe}', '{source_file_keys_str}', "
            f"{total_upserted}, current_timestamp())"
        )
        logger.info(
            f"Recorded load completion in {loads_table}: "
            f"{total_upserted} rows, dag_run_id={dag_run_id}"
        )

        logger.info(f"Total upserted to {full_table_name}: {total_upserted} rows")
        return total_upserted
    finally:
        cursor.close()


def read_databricks_table(
    query: str,
    databricks_conn_id_var: str = "databricks_conn_id",
    batch_size: int = 5_000,
    use_cloud_fetch: bool = False,
) -> Tuple[List[str], Generator[List[tuple], None, None]]:
    """Stream batches of rows from Databricks for memory-bounded reads.

    Args:
        query: SQL SELECT statement to execute.
        databricks_conn_id_var: Airflow Variable holding the Databricks connection ID.
        batch_size: Number of rows per batch (fetchmany size).
        use_cloud_fetch: Enable CloudFetch (bulk S3 download). Disabled by
            default so that fetchmany controls peak memory usage.

    Returns:
        (column_names, batch_iterator) — column_names is a list of strings,
        batch_iterator yields lists of row tuples.
    """
    db_conn_id = Variable.get(databricks_conn_id_var)
    db_conn = BaseHook.get_connection(db_conn_id)

    connection = get_databricks_connection(
        host=db_conn.host,
        http_path=db_conn.extra_dejson.get("http_path", ""),
        client_id=db_conn.login,
        client_secret=db_conn.password,
        use_cloud_fetch=use_cloud_fetch,
    )

    try:
        cursor = connection.cursor()
        # Retry transient 5xx errors (e.g. 503 during SQL warehouse cold start
        # where the connect handshake succeeds but the first statement POST
        # races the warehouse coming online).
        max_retries = 6
        retry_delay = 30
        for attempt in range(max_retries):
            try:
                cursor.execute(query)
                break
            except Exception as e:
                msg = str(e)
                transient = "status code 5" in msg or "Service Unavailable" in msg
                if not transient or attempt == max_retries - 1:
                    raise
                logger.warning(
                    "Databricks execute attempt %d/%d hit transient error: %s. "
                    "Retrying in %ds...",
                    attempt + 1,
                    max_retries,
                    msg,
                    retry_delay,
                )
                time.sleep(retry_delay)
        column_names = [desc[0] for desc in cursor.description]
    except Exception:
        connection.close()
        raise

    def _batch_iterator():
        try:
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                yield batch
        finally:
            cursor.close()
            connection.close()
            logger.info("Databricks connection closed")

    return column_names, _batch_iterator()
