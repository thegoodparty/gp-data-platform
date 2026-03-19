"""Tests for databricks_io utilities."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from databricks_io import _df_to_databricks_schema, is_databricks_fqn, write_table


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
    assert "`age` BIGINT" in schema
    assert "`score` DOUBLE" in schema
    assert "`active` BOOLEAN" in schema


@patch("databricks_io.get_connection")
def test_write_table_fails_without_overwrite(mock_get_conn):
    """write_table raises when table exists and overwrite=False."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,)  # table exists
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn

    df = pd.DataFrame({"col": ["val"]})
    with pytest.raises(RuntimeError, match="already exists"):
        write_table(df, "cat.sch.tbl", overwrite=False)
