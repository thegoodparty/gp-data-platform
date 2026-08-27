import logging
import time
from collections.abc import Generator
from typing import TypedDict

from airflow.sdk import BaseHook, Variable
from databricks import sql as databricks_sql
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql.client import Connection

logger = logging.getLogger("airflow.task")


# OAuth credential failures (rotated/expired service-principal secret, wrong
# client_id) are permanent: retrying cannot fix them, and Databricks tags them
# "non-retryable". Detect them so we fail fast with an actionable message
# instead of burning the full cold-start retry loop (~10 min) behind a
# misleading "warehouse may be starting" log.
_NON_RETRYABLE_AUTH_MARKERS = (
    "invalid_client",
    "client authentication failed",
)


def _is_non_retryable_auth_error(exc: Exception) -> bool:
    """True if the exception is a permanent OAuth credential failure."""
    message = str(exc).lower()
    return any(marker in message for marker in _NON_RETRYABLE_AUTH_MARKERS)


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
            if _is_non_retryable_auth_error(e):
                logger.error(
                    "Databricks OAuth authentication failed (non-retryable): %s. "
                    "The service-principal client_id/secret for this Databricks "
                    "connection is invalid or expired. Rotate the service "
                    "principal's OAuth secret in Databricks and update the Airflow "
                    "connection, then re-run. Not retrying.",
                    e,
                )
                raise
            if attempt == max_retries - 1:
                logger.error(f"Databricks connection failed after {max_retries} attempts: {e}")
                raise
            logger.warning(
                f"Databricks connection attempt {attempt + 1}/{max_retries} failed: {e}. "
                f"Retrying in {retry_delay}s (warehouse may be starting)..."
            )
            time.sleep(retry_delay)
    raise RuntimeError("Unreachable")


class _ConnKwargs(TypedDict):
    host: str
    http_path: str
    client_id: str
    client_secret: str


def _conn_kwargs(databricks_conn_id_var: str = "databricks_conn_id") -> _ConnKwargs:
    """The host and OAuth credentials of the Databricks connection an Airflow Variable names."""
    db_conn_id = Variable.get(databricks_conn_id_var)
    db_conn = BaseHook.get_connection(db_conn_id)

    http_path = db_conn.extra_dejson.get("http_path", "")
    if not (db_conn.host and db_conn.login and db_conn.password and http_path):
        raise ValueError(
            f"Databricks connection '{db_conn_id}' is missing a required "
            "host, login, password, or http_path (extra) field"
        )

    return {
        "host": db_conn.host,
        "http_path": http_path,
        "client_id": db_conn.login,
        "client_secret": db_conn.password,
    }


def connect_from_conn_id(
    databricks_conn_id_var: str = "databricks_conn_id",
    use_cloud_fetch: bool = False,
) -> Connection:
    """Connect to the Databricks warehouse an Airflow Variable names."""
    return get_databricks_connection(**_conn_kwargs(databricks_conn_id_var), use_cloud_fetch=use_cloud_fetch)


def read_databricks_table(
    query: str,
    databricks_conn_id_var: str = "databricks_conn_id",
    batch_size: int = 5_000,
    use_cloud_fetch: bool = False,
) -> tuple[list[str], Generator[list[tuple], None, None]]:
    """Stream batches of rows from Databricks for memory-bounded reads.

    Args:
        query: SQL SELECT statement to execute.
        databricks_conn_id_var: Airflow Variable holding the Databricks connection ID.
        batch_size: Rows per batch, passed as the cursor's `arraysize` so it
            also bounds what the connector holds resident.
        use_cloud_fetch: Enable CloudFetch (bulk S3 download). Disabled by
            default so that fetchmany controls peak memory usage.

    Returns:
        (column_names, batch_iterator) — column_names is a list of strings,
        batch_iterator yields lists of row tuples.
    """
    connection = connect_from_conn_id(databricks_conn_id_var, use_cloud_fetch=use_cloud_fetch)

    try:
        # arraysize is the server fetch size, and defaults to 100,000 rows;
        # fetchmany only slices an already-resident buffer.
        cursor = connection.cursor(arraysize=batch_size)
        execute_with_retry(cursor, query)
        if cursor.description is None:
            raise RuntimeError("Databricks cursor returned no description after execute")
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


def execute_with_retry(
    cursor, query, parameters: dict | None = None, max_retries: int = 6, retry_delay: int = 30
) -> None:
    """Execute `query`, retrying transient 5xx errors (e.g. warehouse cold start)."""
    for attempt in range(max_retries):
        try:
            cursor.execute(query, parameters)
            return
        except Exception as e:
            msg = str(e)
            transient = "status code 5" in msg or "Service Unavailable" in msg
            if not transient or attempt == max_retries - 1:
                raise
            logger.warning(
                "Databricks execute attempt %d/%d hit transient error: %s. Retrying in %ds...",
                attempt + 1,
                max_retries,
                msg,
                retry_delay,
            )
            time.sleep(retry_delay)


def read_databricks_partitioned(
    base_query: str,
    partition_column: str,
    databricks_conn_id_var: str = "databricks_conn_id",
    batch_size: int = 5_000,
    use_cloud_fetch: bool = False,
) -> Generator[list[tuple], None, None]:
    """Stream `base_query` one distinct `partition_column` value at a time over a
    SINGLE Databricks connection.

    Peak memory is bounded by `batch_size`, which sets the cursor's `arraysize`
    and so the resident buffer; partitioning keeps each server-side result set
    small rather than capping what the client holds. Exactly one connection is
    opened for the entire read. Opening a fresh connection per partition (the
    naive loop) leaks resources that accumulate across many partitions and OOM
    the worker on large reads.

    Yields lists of row tuples (same batch shape as read_databricks_table). The
    connection is closed when the generator is exhausted or closed.
    """
    # Read here so a bad connection fails at call time, but connected inside the generator so a
    # caller that never iterates opens nothing.
    conn_kwargs = _conn_kwargs(databricks_conn_id_var)

    def _iter():
        connection = get_databricks_connection(**conn_kwargs, use_cloud_fetch=use_cloud_fetch)
        cursor = connection.cursor(arraysize=batch_size)
        try:
            execute_with_retry(
                cursor, f"SELECT DISTINCT {partition_column} AS _pv FROM ({base_query}) AS _src"
            )
            values = [row[0] for row in cursor.fetchall()]
            for value in values:
                if value is None:
                    predicate = f"_src.{partition_column} IS NULL"
                else:
                    escaped = str(value).replace("'", "''")
                    predicate = f"_src.{partition_column} = '{escaped}'"
                query = f"SELECT * FROM ({base_query}) AS _src WHERE {predicate}"
                logger.info("Reading from Databricks: %s", query)
                execute_with_retry(cursor, query)
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    yield batch
        finally:
            cursor.close()
            connection.close()
            logger.info("Databricks connection closed")

    return _iter()
