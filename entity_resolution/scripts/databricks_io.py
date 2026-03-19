"""
Databricks I/O for entity resolution pipeline.

Reads/writes DataFrames from/to Databricks SQL warehouses.

Authentication (checked in order):
    1. OAuth M2M (service principal) — if DATABRICKS_CLIENT_ID and
       DATABRICKS_CLIENT_SECRET are set alongside DATABRICKS_HOST.
       Used in production containers.
    2. Databricks CLI / SDK default auth — falls back to ~/.databrickscfg,
       used for local development.

Required env vars (both modes):
    DATABRICKS_HTTP_PATH     — e.g. /sql/1.0/warehouses/abcdef1234567890

For OAuth M2M (production):
    DATABRICKS_HOST          — e.g. dbc-abc123.cloud.databricks.com
    DATABRICKS_CLIENT_ID     — service principal application (client) ID
    DATABRICKS_CLIENT_SECRET — service principal secret

For CLI auth (local dev):
    Run `databricks configure` or `databricks auth login` first.
    DATABRICKS_HOST is optional — will be read from CLI profile if not set.
"""

import os
import time

import pandas as pd
from databricks import sql as databricks_sql
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql.client import Connection

# Pandas dtype -> Databricks SQL type mapping, since these don't get correctly
# mapped all the time
_DTYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "DOUBLE",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "STRING",
}


def is_databricks_fqn(value: str) -> bool:
    """Check if a string looks like a Databricks fully-qualified table name.

    (catalog.schema.table). Useful for determining whether the input is a CSV
    file or a table.
    """
    if os.sep in value or value.endswith(".csv"):
        return False
    parts = value.split(".")
    return len(parts) == 3 and all(p.strip() for p in parts)


def _strip_scheme(url: str) -> str:
    """Remove https:// or http:// prefix from a URL."""
    return url.removeprefix("https://").removeprefix("http://")


def _build_connect_kwargs() -> dict:
    """Resolve auth method and return kwargs for databricks_sql.connect().

    Tries OAuth M2M first (env vars), then falls back to Databricks SDK
    unified auth (CLI profile, Azure CLI, etc.).
    """
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    if not http_path:
        raise ValueError("DATABRICKS_HTTP_PATH env var is required")

    client_id = os.environ.get("DATABRICKS_CLIENT_ID")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
    host = os.environ.get("DATABRICKS_HOST", "")

    if client_id and client_secret:
        # Production: OAuth M2M via service principal
        hostname = _strip_scheme(host)
        if not hostname:
            raise ValueError(
                "DATABRICKS_HOST is required when using OAuth M2M "
                "(DATABRICKS_CLIENT_ID/SECRET are set)"
            )

        def credential_provider():
            config = Config(
                host=f"https://{hostname}",
                client_id=client_id,
                client_secret=client_secret,
            )
            return oauth_service_principal(config)

        print("Auth: OAuth M2M (service principal)")
        return {
            "server_hostname": hostname,
            "http_path": http_path,
            "credentials_provider": credential_provider,
        }
    else:
        # Local dev: Databricks SDK unified auth (CLI profile, PAT, etc.)
        config = Config(
            host=f"https://{_strip_scheme(host)}" if host else None,
        )
        hostname = _strip_scheme(config.host)
        print(f"Auth: Databricks CLI / SDK default ({hostname})")
        # credentials_provider must return a HeaderFactory (callable -> dict).
        # config.authenticate is itself a method that returns a dict, so we
        # wrap it in a lambda to match the expected () -> (() -> dict) signature.
        return {
            "server_hostname": hostname,
            "http_path": http_path,
            "credentials_provider": lambda: config.authenticate,
        }


def get_connection(
    max_retries: int = 20,
    retry_delay: int = 30,
) -> Connection:
    """Create a Databricks connection with cold-start retry."""
    connect_kwargs = _build_connect_kwargs()

    for attempt in range(max_retries):
        try:
            connection = databricks_sql.connect(**connect_kwargs)
            print("Databricks connection established")
            return connection
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(
                f"Connection attempt {attempt + 1}/{max_retries} failed: {e}. "
                f"Retrying in {retry_delay}s..."
            )
            time.sleep(retry_delay)
    raise RuntimeError("Unreachable")


def _parse_fqn(fqn: str) -> tuple[str, str, str]:
    """Parse catalog.schema.table from a fully-qualified name."""
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected catalog.schema.table, got: {fqn}")
    return parts[0], parts[1], parts[2]


def read_table(fqn: str) -> pd.DataFrame:
    """Read a Databricks table into a pandas DataFrame."""
    catalog, schema, table = _parse_fqn(fqn)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM `{catalog}`.`{schema}`.`{table}`")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        cursor.close()
        df = pd.DataFrame(rows, columns=columns)
        print(f"Read {len(df):,} rows from {fqn}")
        return df
    finally:
        conn.close()


def _df_to_databricks_schema(df: pd.DataFrame) -> str:
    """Convert DataFrame dtypes to a Databricks CREATE TABLE column spec."""
    cols = []
    for col_name, dtype in df.dtypes.items():
        db_type = _DTYPE_MAP.get(str(dtype), "STRING")
        cols.append(f"`{col_name}` {db_type}")
    return ", ".join(cols)


def _table_exists(cursor, catalog: str, schema: str, table: str) -> bool:
    """Check if a table exists via information_schema."""
    cursor.execute(
        f"SELECT 1 FROM `{catalog}`.information_schema.tables "
        "WHERE table_catalog = ? "
        "AND table_schema = ? "
        "AND table_name = ?",
        parameters=[catalog, schema, table],
    )
    return cursor.fetchone() is not None


def write_table(
    df: pd.DataFrame,
    fqn: str,
    overwrite: bool = False,
    batch_size: int = 5_000,
) -> None:
    """Write a pandas DataFrame to a Databricks table via batched INSERT."""
    # This will be sufficient for small batches of records (<100k), but past
    # that we should rework this to upload a parquet file to a Databricks Volume
    # and then execute a COPY INTO.
    catalog, schema, table = _parse_fqn(fqn)
    conn = get_connection()
    try:
        cursor = conn.cursor()

        if not overwrite and _table_exists(cursor, catalog, schema, table):
            raise RuntimeError(
                f"Table {fqn} already exists. Use --overwrite to replace it."
            )

        cursor.execute(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table}`")
        schema_spec = _df_to_databricks_schema(df)
        cursor.execute(f"CREATE TABLE `{catalog}`.`{schema}`.`{table}` ({schema_spec})")
        print(f"Created table {fqn}")

        # Batched INSERT via VALUES clause
        total = 0
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size]
            rows_sql = []
            for _, row in batch.iterrows():
                vals = []
                for v in row:
                    if (
                        v is None
                        or (isinstance(v, float) and pd.isna(v))
                        or v is pd.NaT
                    ):
                        vals.append("NULL")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        escaped = str(v).replace("\\", "\\\\").replace("'", "\\'")
                        vals.append(f"'{escaped}'")
                rows_sql.append(f"({', '.join(vals)})")
            values_clause = ", ".join(rows_sql)
            cursor.execute(
                f"INSERT INTO `{catalog}`.`{schema}`.`{table}` VALUES {values_clause}"
            )
            total += len(batch)
            print(
                f"  Inserted batch {i // batch_size + 1} ({total:,}/{len(df):,} rows)"
            )

        cursor.close()
        print(f"Wrote {total:,} rows to {fqn}")
    finally:
        conn.close()
