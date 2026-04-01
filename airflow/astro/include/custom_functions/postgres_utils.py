"""Shared utilities for writing to the people-api PostgreSQL database via SSH tunnel.

Designed to be reused across all people-api table writes:
DistrictStats, District, DistrictVoter, Voter.
"""

import logging
from contextlib import contextmanager
from io import StringIO
from typing import List, Optional, Sequence

import paramiko
import psycopg2
import psycopg2.extras

# Compatibility: paramiko ≥3 removed DSSKey (DSA is deprecated),
# but sshtunnel still references it during host-key discovery.
if not hasattr(paramiko, "DSSKey"):

    class _DSSKeyStub:
        @staticmethod
        def from_private_key_file(*a, **kw):
            raise paramiko.SSHException("DSA not supported")

    paramiko.DSSKey = _DSSKeyStub  # type: ignore[attr-defined]

from sshtunnel import SSHTunnelForwarder

from airflow.sdk import BaseHook

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

    # Build SSH auth kwargs — prefer key-based auth, fall back to password.
    ssh_kwargs: dict = {}
    pem_data = bastion.extra_dejson.get("private_key", "")
    passphrase = (
        bastion.extra_dejson.get("private_key_passphrase") or bastion.password or None
    )
    if pem_data:
        # Try each key class until one parses the key successfully.
        pkey = None
        key_classes = [
            paramiko.Ed25519Key,
            paramiko.RSAKey,
            paramiko.ECDSAKey,
        ]
        for cls in key_classes:
            try:
                pkey = cls.from_private_key(  # type: ignore[attr-defined]
                    StringIO(pem_data), password=passphrase
                )
                logger.info("Loaded SSH key: %s", cls.__name__)
                break
            except (paramiko.SSHException, ValueError):
                continue
        if pkey is None:
            raise ValueError("Could not load SSH key with any supported key type")
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


def get_max_updated_at(conn, schema: str, table: str) -> Optional[str]:
    """Return the MAX(updated_at) from a PostgreSQL table as an ISO string.

    Returns None if the table is empty.
    """
    cur = conn.cursor()
    try:
        cur.execute(f'SELECT MAX("updated_at") FROM "{schema}"."{table}"')
        result = cur.fetchone()[0]
        if result is None:
            return None
        return result.isoformat()
    finally:
        cur.close()


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

    if update_cols:
        update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        conflict_action = f"DO UPDATE SET {update_clause}"
    else:
        conflict_action = "DO NOTHING"

    sql = (
        f'INSERT INTO "{schema}"."{table}" ({col_list}) '
        f"VALUES %s "
        f"ON CONFLICT ({conflict_list}) {conflict_action}"
    )

    cur = conn.cursor()
    try:
        psycopg2.extras.execute_values(cur, sql, rows, page_size=batch_size)
        conn.commit()
        logger.info("Upserted %d rows into %s", len(rows), table)
    finally:
        cur.close()

    return len(rows)
