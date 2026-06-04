"""Tests for databricks_io utilities."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from scripts.databricks_io import (
    _df_to_databricks_schema,
    is_databricks_fqn,
    write_table,
)


def test_is_databricks_fqn_valid():
    assert is_databricks_fqn("catalog.schema.table") is True
    assert is_databricks_fqn("my_catalog.my_schema.my_table") is True


def test_is_databricks_fqn_csv_path():
    assert is_databricks_fqn("data/input.csv") is False
    assert is_databricks_fqn("/abs/path/to/file.csv") is False


def test_is_databricks_fqn_too_few_dots():
    assert is_databricks_fqn("schema.table") is False
    assert is_databricks_fqn("table") is False


def test_is_databricks_fqn_with_path_separator():
    assert is_databricks_fqn("path/catalog.schema.table") is False


def test_df_to_databricks_schema():
    # matcha standardizes on string-typed outputs: every column is STRING
    # regardless of pandas dtype. Downstream dbt staging models cast as needed.
    df = pd.DataFrame(
        {
            "name": ["alice"],
            "age": [30],
            "score": [0.95],
            "active": [True],
        }
    )
    schema = _df_to_databricks_schema(df)
    assert "`name` STRING" in schema
    assert "`age` STRING" in schema
    assert "`score` STRING" in schema
    assert "`active` STRING" in schema


def test_parquet_schema_coerces_null_columns_to_string(tmp_path):
    """All-null columns are written as string type (not null type) in parquet."""
    df = pd.DataFrame(
        {"name": ["alice", "bob"], "all_null": [None, None], "score": [0.9, 0.8]}
    )
    outpath = tmp_path / "test.parquet"

    # Replicate the schema logic from write_table
    inferred = pa.Schema.from_pandas(df, preserve_index=False)
    fields = [
        pa.field(f.name, pa.string(), nullable=True) if f.type == pa.null() else f
        for f in inferred
    ]
    schema = pa.schema(fields)
    df.to_parquet(outpath, index=False, schema=schema)

    result_schema = pq.read_schema(outpath)
    # The all-null column should be string, not null
    assert result_schema.field("all_null").type == pa.string()
    # Other columns keep their original types
    assert result_schema.field("name").type == pa.string()
    assert result_schema.field("score").type == pa.float64()

    # Null values are preserved (not empty strings)
    result_df = pd.read_parquet(outpath)
    assert result_df["all_null"].isna().all()


def test_parquet_schema_handles_named_index(tmp_path):
    """DataFrames with named or non-default indexes don't break parquet write."""
    df = pd.DataFrame({"x": [1, 2]}).set_index(pd.Index(["a", "b"], name="idx"))
    outpath = tmp_path / "test.parquet"

    inferred = pa.Schema.from_pandas(df, preserve_index=False)
    fields = [
        pa.field(f.name, pa.string(), nullable=True) if f.type == pa.null() else f
        for f in inferred
    ]
    schema = pa.schema(fields)

    # Should not raise ValueError about index fields
    df.to_parquet(outpath, index=False, schema=schema)
    result_df = pd.read_parquet(outpath)
    assert list(result_df.columns) == ["x"]
    assert len(result_df) == 2


def test_parquet_schema_does_not_mutate_dataframe():
    """The schema construction does not modify the original DataFrame."""
    df = pd.DataFrame({"val": ["a", "b"], "empty": [None, None]})
    original_dtypes = df.dtypes.copy()

    inferred = pa.Schema.from_pandas(df, preserve_index=False)
    fields = [
        pa.field(f.name, pa.string(), nullable=True) if f.type == pa.null() else f
        for f in inferred
    ]
    pa.schema(fields)

    # DataFrame should be unchanged
    assert df["empty"].isna().all()
    assert (df.dtypes == original_dtypes).all()


@patch("scripts.databricks_io.WorkspaceClient")
@patch("scripts.databricks_io.get_connection")
def test_write_table_fails_without_overwrite(mock_get_conn, _mock_ws):
    """write_table raises when table exists and overwrite=False."""
    mock_cursor = MagicMock()
    # Simulate Databricks raising on CREATE TABLE when table already exists
    mock_cursor.execute.side_effect = Exception(
        "[TABLE_OR_VIEW_ALREADY_EXISTS] Table cat.sch.tbl already exists."
    )
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn

    df = pd.DataFrame({"col": ["val"]})
    with pytest.raises(RuntimeError, match="already exists"):
        write_table(df, "cat.sch.tbl", overwrite=False)
