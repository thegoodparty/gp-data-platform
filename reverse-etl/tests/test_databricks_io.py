from __future__ import annotations

from collections.abc import Sequence

import pytest

from retl.databricks_io import DatabricksConnConfig, config_from_env, fetch_all_rows


class _FakeCursorForFetch:
    """A minimal cursor: proves fetch_all_rows drains in explicit-size batches."""

    def __init__(self, rows: Sequence[tuple[object, ...]], columns: list[str]):
        self._rows = rows
        self.description = [(name,) for name in columns]
        self._position = 0

    def fetchmany(self, size: int) -> list[tuple[object, ...]]:
        batch = list(self._rows[self._position : self._position + size])
        self._position += len(batch)
        return batch


def test_fetch_all_rows_drains_multiple_batches() -> None:
    """Catches: only the first fetchmany() batch being read, silently dropping later rows."""
    rows = [(f"p{i}",) for i in range(5)]
    cursor = _FakeCursorForFetch(rows, ["tracking_key"])
    result = fetch_all_rows(cursor, batch_size=2)
    assert [row["tracking_key"] for row in result] == [f"p{i}" for i in range(5)]


def test_fetch_all_rows_maps_columns_by_cursor_description_order() -> None:
    """Catches: a positional row being zipped against the wrong column names."""
    cursor = _FakeCursorForFetch([("p1", '{"a":1}')], ["tracking_key", "payload"])
    result = fetch_all_rows(cursor, batch_size=10)
    assert result == [{"tracking_key": "p1", "payload": '{"a":1}'}]


def test_config_from_env_strips_the_https_scheme_from_host() -> None:
    """Catches: passing a full URL where the connector expects a bare hostname."""
    config = config_from_env(
        {"DATABRICKS_HOST": "https://dbc-example.cloud.databricks.com/", "DATABRICKS_HTTP_PATH": "/sql/1.0/x"}
    )
    assert config.server_hostname == "dbc-example.cloud.databricks.com"


def test_config_from_env_requires_host_and_http_path() -> None:
    """Catches: a missing DATABRICKS_HOST or HTTP_PATH being silently treated as configured."""
    with pytest.raises(ValueError, match="DATABRICKS_HOST"):
        config_from_env({"DATABRICKS_HTTP_PATH": "/sql/1.0/x"})


def test_config_from_env_reads_token_and_client_credentials() -> None:
    """Catches: the client id/secret OAuth M2M path (load_people_api.py's own pattern) being dropped."""
    config = config_from_env(
        {
            "DATABRICKS_HOST": "dbc-example.cloud.databricks.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/abc",
            "DATABRICKS_CLIENT_ID": "cid",
            "DATABRICKS_CLIENT_SECRET": "csecret",
        }
    )
    assert config == DatabricksConnConfig(
        server_hostname="dbc-example.cloud.databricks.com",
        http_path="/sql/1.0/warehouses/abc",
        access_token=None,
        client_id="cid",
        client_secret="csecret",
    )
