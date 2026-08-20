"""Tests for databricks_io utilities."""

import datetime
import io
import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.databricks_io import (
    _build_connect_kwargs,
    _coerce_to_string_df,
    _df_to_databricks_schema,
    get_connection,
    is_databricks_fqn,
    read_table,
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
    # matcha standardizes Databricks output on STRING-typed columns (downstream
    # dbt staging casts back); _df_to_databricks_schema reflects that.
    assert "`name` STRING" in schema
    assert "`age` STRING" in schema
    assert "`score` STRING" in schema
    assert "`active` STRING" in schema


def test_parquet_schema_coerces_null_columns_to_string(tmp_path):
    """All-null columns are written as string type (not null type) in parquet."""
    df = pd.DataFrame({"name": ["alice", "bob"], "all_null": [None, None], "score": [0.9, 0.8]})
    outpath = tmp_path / "test.parquet"

    # The schema logic from write_table: every column pinned to string, so the
    # COPY INTO never merges a null/numeric field into the STRING table schema.
    # Pinned rather than inferred, which is why the pandas dtype behind a column
    # (object vs. str, and so string vs. large_string) cannot change what lands.
    coerced = _coerce_to_string_df(df)
    schema = pa.schema([pa.field(name, pa.string(), nullable=True) for name in coerced.columns])
    coerced.to_parquet(outpath, index=False, schema=schema)

    result_schema = pq.read_schema(outpath)
    # Every column is string -- the all-null one included, never null type
    assert [f.type for f in result_schema] == [pa.string()] * 3

    # Null values are preserved (not empty strings, not the text "nan")
    result_df = pd.read_parquet(outpath)
    assert result_df["all_null"].isna().all()
    assert result_df["name"].tolist() == ["alice", "bob"]


def test_parquet_schema_handles_named_index(tmp_path):
    """DataFrames with named or non-default indexes don't break parquet write."""
    df = pd.DataFrame({"x": [1, 2]}).set_index(pd.Index(["a", "b"], name="idx"))
    outpath = tmp_path / "test.parquet"

    inferred = pa.Schema.from_pandas(df, preserve_index=False)
    fields = [pa.field(f.name, pa.string(), nullable=True) if f.type == pa.null() else f for f in inferred]
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
    fields = [pa.field(f.name, pa.string(), nullable=True) if f.type == pa.null() else f for f in inferred]
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


def test_coerce_to_string_df_datetime_nulls_stay_null():
    """A null in a datetime column must not serialize as the text "NaT".

    pd.NaT is neither None nor a float, so a float-only isna guard lets
    str(NaT) through and writes the literal "NaT" into an all-STRING Delta
    table. Asserts null-ness rather than a specific sentinel: the value is
    None on pandas 2 and NaN once the column infers as str on pandas 3, and
    both land as SQL NULL.
    """
    df = pd.DataFrame({"election_date": pd.to_datetime(["2026-11-03", None])})

    out = _coerce_to_string_df(df)

    assert out["election_date"].iloc[0] == "2026-11-03 00:00:00"
    assert pd.isna(out["election_date"].iloc[1])
    assert "NaT" not in out["election_date"].tolist()


def test_coerce_to_string_df_pd_na_stays_null():
    """pd.NA is a null, not the text "<NA>"."""
    df = pd.DataFrame({"c": pd.array(["x", None], dtype="string")})

    out = _coerce_to_string_df(df)

    assert out["c"].iloc[0] == "x"
    assert pd.isna(out["c"].iloc[1])
    assert "<NA>" not in out["c"].tolist()


# --- _coerce_to_string_df value matrix ----------------------------------------
# matcha persists all-STRING Delta tables and lets dbt staging cast back, so any
# drift in these strings silently changes warehouse values.


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (5.0, "5"),  # whole floats lose ".0" so BIGINT-shaped data round-trips
        (-0.0, "-0"),
        (0.9, "0.9"),
        (2**53 + 1, "9007199254740993"),  # beyond float precision, stays exact
        (-(2**40), "-1099511627776"),
        (1e16, "1e+16"),
        (1e-07, "1e-07"),
        (float("inf"), "inf"),
        (True, "True"),
        (False, "False"),
        ("Ω unicode", "Ω unicode"),
        ("trailing.0", "trailing"),  # documents that the strip is unconditional
        (datetime.date(2026, 11, 3), "2026-11-03"),
    ],
)
def test_coerce_to_string_df_value_matrix(value, expected):
    out = _coerce_to_string_df(pd.DataFrame({"c": [value]}))
    assert out["c"].tolist() == [expected]


@pytest.mark.parametrize("null", [None, float("nan")])
def test_coerce_to_string_df_scalar_nulls(null):
    out = _coerce_to_string_df(pd.DataFrame({"c": [null]}))
    assert pd.isna(out["c"].iloc[0])
    assert not any(isinstance(v, str) for v in out["c"].tolist())


def test_coerce_to_string_df_serializes_containers():
    """list and ndarray cells become JSON, per write_table's all-string contract."""
    df = pd.DataFrame({"c": [["x", "y"], np.array([1, 2])]})
    out = _coerce_to_string_df(df)
    assert [json.loads(v) for v in out["c"]] == [["x", "y"], [1, 2]]


def test_coerce_to_string_df_does_not_mutate_input():
    df = pd.DataFrame({"c": [1.0, None]})
    before = df["c"].tolist()
    _coerce_to_string_df(df)
    assert df["c"].tolist()[0] == before[0]
    assert str(df["c"].dtype) == "float64"


# --- write_table: the staged-parquet contract ---------------------------------


def _mock_write_table_env(mock_get_conn, mock_ws):
    """Wire the mocks and capture the parquet bytes handed to files.upload()."""
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mock_get_conn.return_value = conn

    captured = {}

    def _upload(path, fh, overwrite=False):
        captured["path"] = path
        captured["data"] = fh.read()

    mock_ws.return_value.files.upload.side_effect = _upload
    return cursor, captured


@patch("scripts.databricks_io.WorkspaceClient")
@patch("scripts.databricks_io.get_connection")
def test_write_table_stages_all_string_parquet(mock_get_conn, mock_ws):
    """The staged parquet pins every column to string and preserves nulls.

    This is the invariant that makes the write path independent of the pandas
    dtype behind each column: the schema is pinned, never inferred, so a column
    backed by object or str (and so arrow string or large_string) lands the same.
    """
    cursor, captured = _mock_write_table_env(mock_get_conn, mock_ws)
    df = pd.DataFrame({"name": ["alice", "bob"], "all_null": [None, None], "score": [0.9, 0.8]})

    write_table(df, "cat.sch.tbl", overwrite=True)

    schema = pq.read_schema(io.BytesIO(captured["data"]))
    assert [f.type for f in schema] == [pa.string()] * 3

    back = pd.read_parquet(io.BytesIO(captured["data"]))
    assert back["all_null"].isna().all()
    assert back["name"].tolist() == ["alice", "bob"]
    assert back["score"].tolist() == ["0.9", "0.8"]


@patch("scripts.databricks_io.WorkspaceClient")
@patch("scripts.databricks_io.get_connection")
def test_write_table_issues_expected_sql(mock_get_conn, mock_ws):
    """CREATE OR REPLACE with all-STRING columns, then COPY INTO the staged file."""
    cursor, captured = _mock_write_table_env(mock_get_conn, mock_ws)

    write_table(pd.DataFrame({"col": ["v"], "n": [1]}), "cat.sch.tbl", overwrite=True)

    sql = " ".join(c.args[0] for c in cursor.execute.call_args_list)
    assert "CREATE OR REPLACE TABLE" in sql
    assert "`col` STRING" in sql
    assert "`n` STRING" in sql
    assert "CREATE VOLUME IF NOT EXISTS" in sql
    assert "COPY INTO" in sql
    assert "FILEFORMAT = PARQUET" in sql


@patch("scripts.databricks_io.WorkspaceClient")
@patch("scripts.databricks_io.get_connection")
def test_write_table_cleans_up_staged_file_and_connection(mock_get_conn, mock_ws):
    cursor, captured = _mock_write_table_env(mock_get_conn, mock_ws)

    write_table(pd.DataFrame({"col": ["v"]}), "cat.sch.tbl", overwrite=True)

    mock_ws.return_value.files.delete.assert_called_once_with(captured["path"])
    mock_get_conn.return_value.close.assert_called_once()


# --- read_table: the Arrow-to-pandas input path -------------------------------


@patch("scripts.databricks_io.get_connection")
def test_read_table_converts_arrow_to_dataframe(mock_get_conn):
    """read_table returns fetchall_arrow().to_pandas() and closes the connection."""
    cursor = MagicMock()
    cursor.fetchall_arrow.return_value = pa.table(
        {"unique_id": pa.array(["1", "2"], pa.string()), "n": pa.array([1, None], pa.int64())}
    )
    conn = MagicMock()
    conn.cursor.return_value = cursor
    mock_get_conn.return_value = conn

    df = read_table("cat.sch.tbl")

    assert list(df.columns) == ["unique_id", "n"]
    assert df["unique_id"].tolist() == ["1", "2"]
    assert pd.isna(df["n"].iloc[1])
    assert "SELECT * FROM" in cursor.execute.call_args.args[0]
    conn.close.assert_called_once()


# --- connection setup: auth selection and cold-start retry ---------------------


@patch.dict("os.environ", {"DATABRICKS_HTTP_PATH": ""}, clear=False)
def test_build_connect_kwargs_requires_http_path():
    with pytest.raises(ValueError, match="DATABRICKS_HTTP_PATH"):
        _build_connect_kwargs()


@patch("scripts.databricks_io.Config")
@patch.dict("os.environ", {"DATABRICKS_HTTP_PATH": "sql/1.0/warehouses/abc"}, clear=False)
def test_build_connect_kwargs_strips_scheme_from_host(mock_config):
    """server_hostname must be bare; the SDK returns host with a scheme."""
    mock_config.return_value = MagicMock(
        host="https://dbc-123.cloud.databricks.com", client_id=None, client_secret=None
    )

    kwargs = _build_connect_kwargs()

    assert kwargs["server_hostname"] == "dbc-123.cloud.databricks.com"
    assert kwargs["http_path"] == "sql/1.0/warehouses/abc"
    assert callable(kwargs["credentials_provider"])


@patch("scripts.databricks_io.oauth_service_principal")
@patch("scripts.databricks_io.Config")
@patch.dict("os.environ", {"DATABRICKS_HTTP_PATH": "sql/1.0/warehouses/abc"}, clear=False)
def test_build_connect_kwargs_prefers_service_principal(mock_config, mock_oauth):
    """With a client id and secret present, auth goes through OAuth M2M."""
    mock_config.return_value = MagicMock(host="https://h", client_id="cid", client_secret="secret")

    kwargs = _build_connect_kwargs()
    kwargs["credentials_provider"]()

    mock_oauth.assert_called_once_with(mock_config.return_value)


@patch("scripts.databricks_io.Config")
@patch.dict("os.environ", {"DATABRICKS_HTTP_PATH": "sql/1.0/warehouses/abc"}, clear=False)
def test_build_connect_kwargs_falls_back_to_sdk_default(mock_config):
    """Without a secret, the provider hands back the SDK's authenticate callable."""
    cfg = MagicMock(host="https://h", client_id=None, client_secret=None)
    mock_config.return_value = cfg

    provider = _build_connect_kwargs()["credentials_provider"]

    assert provider() is cfg.authenticate


@patch("scripts.databricks_io.time.sleep")
@patch("scripts.databricks_io.databricks_sql.connect")
@patch("scripts.databricks_io._build_connect_kwargs", return_value={})
def test_get_connection_retries_cold_start(_mock_kwargs, mock_connect, mock_sleep):
    """A warehouse waking up fails the first attempts, so connect is retried."""
    conn = MagicMock()
    mock_connect.side_effect = [Exception("cold"), Exception("cold"), conn]

    assert get_connection(max_retries=5, retry_delay=1) is conn
    assert mock_connect.call_count == 3
    assert mock_sleep.call_count == 2


@patch("scripts.databricks_io.time.sleep")
@patch("scripts.databricks_io.databricks_sql.connect")
@patch("scripts.databricks_io._build_connect_kwargs", return_value={})
def test_get_connection_reraises_after_last_attempt(_mock_kwargs, mock_connect, _mock_sleep):
    """The final failure propagates rather than returning None."""
    mock_connect.side_effect = Exception("still cold")

    with pytest.raises(Exception, match="still cold"):
        get_connection(max_retries=2, retry_delay=1)

    assert mock_connect.call_count == 2
