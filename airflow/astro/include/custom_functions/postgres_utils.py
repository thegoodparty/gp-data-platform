"""Shared utilities for writing to the people-api PostgreSQL database via SSH tunnel.

Designed to be reused across all people-api table writes:
DistrictStats, District, DistrictVoter, Voter.
"""

import logging
from contextlib import contextmanager
from io import StringIO
from typing import Iterator, List, Sequence, Tuple

import paramiko
import psycopg2
import psycopg2.extras
from include.custom_functions.databricks_utils import get_databricks_connection
from sshtunnel import SSHTunnelForwarder

from airflow.sdk import BaseHook, Variable

logger = logging.getLogger("airflow.task")


@contextmanager
def get_postgres_via_ssh(
    bastion_conn_id: str = "gp_bastion_host",
    pg_conn_id: str = "people_api_db",
):
    """Open a psycopg2 connection to PostgreSQL through an SSH bastion tunnel.

    Usage::

        with get_postgres_via_ssh() as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")

    Yields:
        psycopg2 connection routed through the SSH tunnel.
    """
    bastion = BaseHook.get_connection(bastion_conn_id)
    pg = BaseHook.get_connection(pg_conn_id)

    # Build SSH auth kwargs — prefer private key, fall back to password.
    ssh_kwargs: dict = {}
    private_key = bastion.extra_dejson.get("private_key", "")
    passphrase = (
        bastion.extra_dejson.get("private_key_passphrase") or bastion.password or None
    )
    if private_key:
        # PKey auto-detects key type and handles the OpenSSH unified format.
        pkey = paramiko.PKey.from_private_key(
            StringIO(private_key), password=passphrase
        )
        logger.info("Loaded SSH key: %s", type(pkey).__name__)
        ssh_kwargs["ssh_pkey"] = pkey
    else:
        ssh_kwargs["ssh_password"] = bastion.password

    tunnel = SSHTunnelForwarder(
        (bastion.host, bastion.port or 22),
        ssh_username=bastion.login,
        **ssh_kwargs,
        remote_bind_address=(pg.host, pg.port or 5432),
        set_keepalive=30,
    )
    conn = None
    try:
        tunnel.start()
        logger.info(
            "SSH tunnel open → localhost:%s → %s:%s",
            tunnel.local_bind_port,
            pg.host,
            pg.port or 5432,
        )
        conn = psycopg2.connect(
            dbname=pg.schema or pg.extra_dejson.get("dbname", pg.login),
            user=pg.login,
            password=pg.password,
            host="127.0.0.1",
            port=tunnel.local_bind_port,
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()
            logger.info("PostgreSQL connection closed")
        tunnel.stop()
        logger.info("SSH tunnel closed")


def upsert_rows(
    conn,
    schema: str,
    table: str,
    columns: List[str],
    conflict_columns: List[str],
    rows: Sequence[tuple],
    batch_size: int = 5000,
) -> int:
    """Batch upsert rows into a PostgreSQL table using INSERT ... ON CONFLICT.

    Args:
        conn: psycopg2 connection.
        schema: PostgreSQL schema name.
        table: Table name (will be double-quoted).
        columns: Ordered column names matching the row tuples.
        conflict_columns: Column(s) forming the conflict target (supports composite keys).
        rows: Iterable of row tuples to upsert.
        batch_size: Rows per INSERT + COMMIT cycle.

    Returns:
        Total number of rows upserted.
    """
    if not rows:
        logger.info("upsert_rows: no rows to upsert")
        return 0

    update_cols = [c for c in columns if c not in conflict_columns]
    col_list = ", ".join(f'"{c}"' for c in columns)
    conflict_list = ", ".join(f'"{c}"' for c in conflict_columns)
    update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

    sql = (
        f'INSERT INTO {schema}."{table}" ({col_list}) '
        f"VALUES %s "
        f"ON CONFLICT ({conflict_list}) DO UPDATE SET {update_clause}"
    )

    total = 0
    cur = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            psycopg2.extras.execute_values(cur, sql, batch, page_size=batch_size)
            conn.commit()
            total += len(batch)
            logger.info(
                "Upserted batch %d (%d rows, %d / %d total)",
                i // batch_size + 1,
                len(batch),
                total,
                len(rows),
            )
    finally:
        cur.close()

    return total


def read_databricks_table(
    query: str,
    databricks_conn_id_var: str = "databricks_conn_id",
    fetch_size: int = 10_000,
) -> Tuple[List[str], Iterator[tuple]]:
    """Stream rows from Databricks using fetchmany for memory-bounded reads.

    Args:
        query: SQL SELECT statement to execute.
        databricks_conn_id_var: Airflow Variable holding the Databricks connection ID.
        fetch_size: Number of rows per fetchmany call.

    Returns:
        (column_names, row_iterator) — column_names is a list of strings,
        row_iterator yields tuples of row values.
    """
    db_conn_id = Variable.get(databricks_conn_id_var)
    db_conn = BaseHook.get_connection(db_conn_id)

    connection = get_databricks_connection(
        host=db_conn.host,
        http_path=db_conn.extra_dejson.get("http_path", ""),
        client_id=db_conn.login,
        client_secret=db_conn.password,
    )

    cursor = connection.cursor()
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]

    def _row_iterator():
        try:
            while True:
                batch = cursor.fetchmany(fetch_size)
                if not batch:
                    break
                yield from batch
        finally:
            cursor.close()
            connection.close()
            logger.info("Databricks connection closed")

    return column_names, _row_iterator()
