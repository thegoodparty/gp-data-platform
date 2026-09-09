from __future__ import annotations

from collections.abc import Sequence
from unittest.mock import patch

import pytest

from retl.databricks_io import DatabricksConnConfig, config_from_env, connect, fetch_all_rows


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


def test_config_from_env_reads_and_trims_the_scopes_variable() -> None:
    """Catches: the variable is declared but never parsed, so a deployment that
    narrowed its service principal still has retl asking for `all-apis` and being
    refused at the token endpoint before it reads a row."""
    config = config_from_env(
        {
            "DATABRICKS_HOST": "dbc.example.com",
            "DATABRICKS_HTTP_PATH": "/sql/1.0/x",
            "DATABRICKS_SCOPES": "  sql, unity-catalog  ",
        }
    )
    assert config.scopes == "sql, unity-catalog"


def test_config_from_env_leaves_scopes_empty_when_unset() -> None:
    """Empty must mean "unchanged": the SDK defaults to `all-apis` on its own, and a
    deployment that narrows nothing must keep working exactly as before."""
    config = config_from_env({"DATABRICKS_HOST": "dbc.example.com", "DATABRICKS_HTTP_PATH": "/x"})
    assert config.scopes == ""


def _connect_capturing_sdk_config(scopes: str):
    """Drive the real `connect()` M2M path with the SDK and driver stubbed out.

    `connect()` is otherwise the one function deliberately left untested because it
    must reach a live warehouse -- but the scope passthrough is precisely what a
    warehouse would never show us until a narrowed service principal refused it.
    """
    config = DatabricksConnConfig(
        server_hostname="dbc.example.com",
        http_path="/sql/1.0/x",
        client_id="cid",
        client_secret="secret",
        scopes=scopes,
    )
    with (
        patch("databricks.sdk.core.Config") as mock_config,
        patch("databricks.sdk.core.oauth_service_principal"),
        patch("databricks.sql.connect"),
    ):
        connect(config)
    return mock_config.call_args.kwargs


def test_connect_passes_the_configured_scopes_to_the_sdk() -> None:
    """Catches the whole point of the variable being dropped on the floor: the SDK
    gives `scopes` no env binding, so a config that carries the value but never hands
    it to Config still requests `all-apis` and is refused."""
    assert _connect_capturing_sdk_config("sql, unity-catalog")["scopes"] == "sql, unity-catalog"


def test_connect_asks_for_nothing_narrower_when_no_scopes_are_configured() -> None:
    """A deployment that narrows nothing must reach the SDK's own `all-apis` default
    rather than an empty value this code invented, which the SDK would parse to a
    different thing than "unset"."""
    assert _connect_capturing_sdk_config("")["scopes"] is None
