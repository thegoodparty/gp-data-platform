"""Databricks connectivity: connection config from environment, reads with an
explicit fetch size.

Configured entirely from environment values the DAG passes: this module never
imports `airflow` and never reads an Airflow Variable or Connection directly,
because the installed console script runs as a bare subprocess with no
task-runner context for those lookups.

Cursors default to `arraysize=100000`, so an unsized `fetchmany()` returns up to
100k rows in one call and bounds nothing by itself. `fetch_all_rows` sets
`.arraysize` explicitly and drains `fetchmany()` in a loop, so the batch size is a
deliberate choice, not the connector's default.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

DEFAULT_FETCH_BATCH_SIZE = 10_000


@dataclass(frozen=True)
class DatabricksConnConfig:
    server_hostname: str
    http_path: str
    access_token: str | None = None
    client_id: str | None = None
    client_secret: str | None = None


def _strip_scheme(host: str) -> str:
    """The connector wants a bare hostname; DATABRICKS_HOST is typically a full URL."""
    return host.removeprefix("https://").removeprefix("http://").rstrip("/")


def config_from_env(env: Mapping[str, str]) -> DatabricksConnConfig:
    host = env.get("DATABRICKS_HOST", "")
    http_path = env.get("DATABRICKS_HTTP_PATH", "")
    if not host or not http_path:
        raise ValueError("DATABRICKS_HOST and DATABRICKS_HTTP_PATH must both be set")
    return DatabricksConnConfig(
        server_hostname=_strip_scheme(host),
        http_path=http_path,
        access_token=env.get("DATABRICKS_TOKEN") or None,
        client_id=env.get("DATABRICKS_CLIENT_ID") or None,
        client_secret=env.get("DATABRICKS_CLIENT_SECRET") or None,
    )


def connect(config: DatabricksConnConfig) -> Any:
    """Open a connection. Not unit tested: it is the one call that must reach a real warehouse.

    Two auth shapes, matching the two forms this repo already passes through env
    (people-api-loader's `load_people_api.py` DAG): a token, used directly by the
    connector's own `access_token` path, or a client id/secret pair, which needs the
    Databricks SDK's OAuth M2M credentials provider -- the sql connector has no
    native (non-Azure) client-id/secret kwarg, only `credentials_provider`.
    """
    import databricks.sql as databricks_sql

    if config.access_token:
        return databricks_sql.connect(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            access_token=config.access_token,
        )
    if config.client_id and config.client_secret:
        from databricks.sdk.core import Config as SdkConfig
        from databricks.sdk.core import oauth_service_principal

        sdk_config = SdkConfig(
            host=config.server_hostname,
            client_id=config.client_id,
            client_secret=config.client_secret,
        )
        return databricks_sql.connect(
            server_hostname=config.server_hostname,
            http_path=config.http_path,
            credentials_provider=lambda: oauth_service_principal(sdk_config),
        )
    raise ValueError("DatabricksConnConfig needs either access_token or client_id and client_secret")


class _Cursor(Protocol):
    description: Any
    arraysize: int

    def fetchmany(self, size: int) -> Any: ...


def fetch_all_rows(cursor: _Cursor, *, batch_size: int = DEFAULT_FETCH_BATCH_SIZE) -> list[dict[str, Any]]:
    """Drain a cursor's result set in explicit-size batches, as a list of dict rows."""
    cursor.arraysize = batch_size
    columns = [col[0] for col in cursor.description]
    rows: list[dict[str, Any]] = []
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            break
        rows.extend(dict(zip(columns, row, strict=True)) for row in batch)
    return rows
