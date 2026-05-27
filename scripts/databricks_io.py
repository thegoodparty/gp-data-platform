"""
Databricks I/O for entity resolution pipeline.

Reads/writes DataFrames from/to Databricks SQL warehouses.

Authentication is resolved by the Databricks SDK Config, which reads
env vars and CLI profiles automatically:

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

import json
import os
import tempfile
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pyarrow as pa
from databricks import sql as databricks_sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql.client import Connection


@dataclass(frozen=True)
class TableFQN:
    """A fully-qualified Databricks table name (catalog.schema.table)."""

    catalog: str
    schema: str
    table: str

    @classmethod
    def parse(cls, fqn: str) -> "TableFQN":
        """Parse a 'catalog.schema.table' string."""
        parts = fqn.split(".")
        if len(parts) != 3 or not all(p.strip() for p in parts):
            raise ValueError(f"Expected catalog.schema.table, got: {fqn}")
        return cls(catalog=parts[0], schema=parts[1], table=parts[2])

    @property
    def quoted(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"


def is_databricks_fqn(value: str) -> bool:
    """Check if a string looks like a Databricks fully-qualified table name
    (catalog.schema.table) vs. a file path.
    """
    if "/" in value or os.sep in value or value.endswith(".csv"):
        return False
    try:
        TableFQN.parse(value)
        return True
    except ValueError:
        return False


def _build_connect_kwargs() -> dict:
    """Return kwargs for databricks_sql.connect().

    Auth is resolved by the Databricks SDK Config, which reads env vars
    (DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET)
    and CLI profiles automatically.
    """
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    if not http_path:
        raise ValueError("DATABRICKS_HTTP_PATH env var is required")

    config = Config()
    hostname = config.host.removeprefix("https://").removeprefix("http://")

    if config.client_id and config.client_secret:
        print("Auth: OAuth M2M (service principal)")
        credentials = lambda: oauth_service_principal(config)
    else:
        print(f"Auth: Databricks CLI / SDK default ({hostname})")
        # Wrap config.authenticate to match the expected
        # () -> (() -> dict) HeaderFactory signature.
        credentials = lambda: config.authenticate

    return {
        "server_hostname": hostname,
        "http_path": http_path,
        "credentials_provider": credentials,
    }


def get_connection(
    max_retries: int = 5,
    retry_delay: int = 10,
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


def read_table(fqn: str) -> pd.DataFrame:
    """Read a Databricks table into a pandas DataFrame."""
    t = TableFQN.parse(fqn)
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {t.quoted}")
        df = cursor.fetchall_arrow().to_pandas()
        cursor.close()
        print(f"Read {len(df):,} rows from {fqn}")
        return df
    finally:
        conn.close()


def _coerce_to_string_df(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with every cell converted to string (or None).

    Matcha persists outputs as all-string Delta tables and lets dbt cast back
    to native types downstream. This mirrors the input-side normalization in
    `cli._normalize_to_strings` and removes the long tail of type-mismatch
    bugs (null-typed parquet fields, list/array columns inferred as
    list<string> by pyarrow, numpy scalars vs floats, bool casing, etc.).

    Rules:
      - None / NaN → None (preserved as SQL NULL)
      - list / np.ndarray → JSON-serialized string
      - everything else → str(...) with `.0` stripped from float-as-int values
        so that BIGINT-shaped data round-trips cleanly
    """

    def _to_str(v):
        if v is None:
            return None
        if isinstance(v, float) and pd.isna(v):
            return None
        if isinstance(v, list):
            return json.dumps(v)
        if isinstance(v, np.ndarray):
            return json.dumps(v.tolist())
        s = str(v)
        # Drop trailing ".0" for whole-number floats so 5.0 -> "5"; mirrors
        # cli._normalize_to_strings on the read side.
        if s.endswith(".0"):
            s = s[:-2]
        return s

    out = df.copy()
    for col in out.columns:
        out[col] = out[col].apply(_to_str)
    return out


def _df_to_databricks_schema(df: pd.DataFrame) -> str:
    """All columns are STRING — matcha standardizes on string-typed outputs."""
    return ", ".join(f"`{c}` STRING" for c in df.columns)


def write_table(
    df: pd.DataFrame,
    fqn: str,
    overwrite: bool = False,
    staging_volume: str = "matcha_staging",
) -> None:
    """Write a pandas DataFrame to a Databricks table via parquet staging.

    All columns are uploaded as STRING. Downstream dbt staging models cast
    back to native types where useful; this keeps the upload pipeline simple
    and avoids the type-merge bugs that plagued mixed-type writes.
    """
    t = TableFQN.parse(fqn)
    df = _coerce_to_string_df(df)
    schema_spec = _df_to_databricks_schema(df)
    w = WorkspaceClient()

    conn = get_connection()
    try:
        cursor = conn.cursor()

        if overwrite:
            cursor.execute(f"CREATE OR REPLACE TABLE {t.quoted} ({schema_spec})")
        else:
            try:
                cursor.execute(f"CREATE TABLE {t.quoted} ({schema_spec})")
            except Exception as e:
                if "already exists" in str(e).lower():
                    raise RuntimeError(
                        f"Table {fqn} already exists. Use --overwrite to replace it."
                    ) from e
                raise
        print(f"Created table {fqn}")

        cursor.execute(
            f"CREATE VOLUME IF NOT EXISTS "
            f"`{t.catalog}`.`{t.schema}`.`{staging_volume}`"
        )

        volume_path = (
            f"/Volumes/{t.catalog}/{t.schema}/{staging_volume}/{t.table}.parquet"
        )
        # Force every parquet column to string so the COPY INTO never has to
        # merge a list / null / numeric field into the STRING table schema.
        schema = pa.schema(
            [pa.field(name, pa.string(), nullable=True) for name in df.columns]
        )

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            df.to_parquet(tmp.name, index=False, schema=schema)
            with open(tmp.name, "rb") as f:
                w.files.upload(volume_path, f, overwrite=True)
            print(f"Uploaded parquet to {volume_path}")

        cursor.execute(
            f"COPY INTO {t.quoted} "
            f"FROM '{volume_path}' "
            f"FILEFORMAT = PARQUET "
            f"COPY_OPTIONS ('mergeSchema' = 'true')"
        )
        print(f"Wrote {len(df):,} rows to {fqn}")

        try:
            w.files.delete(volume_path)
        except Exception:
            pass  # best-effort cleanup

        cursor.close()
    finally:
        conn.close()
