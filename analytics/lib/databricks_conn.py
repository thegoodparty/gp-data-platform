"""Profile-based Databricks connection for analytics notebooks and scripts.

Provides a ``run_query(sql) -> pandas.DataFrame`` callable to inject into the
connection-agnostic helpers in ``win_analysis.py``. It is the analytics
counterpart of ``matcha/scripts/databricks_io.py`` and
``airflow/.../databricks_utils.py``: SQL runs through ``databricks-sql-connector``
and auth is resolved by the Databricks SDK ``Config`` via ``credentials_provider``,
so there is no personal access token and no cross-repo dependency on the
gp-ai-projects ``DatabricksClient``.

Auth mirrors matcha: if the SDK ``Config`` carries a service-principal
``client_id`` / ``client_secret`` (e.g. CI), it uses OAuth M2M; otherwise it uses
the ``~/.databrickscfg`` CLI / SDK default profile (``databricks auth login``,
honoring ``DATABRICKS_CONFIG_PROFILE``). The connector and SDK imports are lazy so
the pure helpers import with only pandas present.

Usage from a notebook::

    import sys; sys.path.insert(0, "analytics/lib")
    import databricks_conn as dbc
    import win_analysis as wa

    df = wa.build_win_working_set(dbc.run_query, cohorts)
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Sequence
from typing import Any

import pandas as pd


def _server_hostname(host: str | None) -> str:
    """Return the bare ``server_hostname`` the sql-connector expects.

    The SDK ``Config.host`` is a full URL (``https://<host>``); the connector
    wants the host with no scheme and no trailing slash.
    """
    if not host:
        raise ValueError("Databricks profile resolved an empty host. Run `databricks auth login`.")
    return host.removeprefix("https://").removeprefix("http://").rstrip("/")


def _build_connect_kwargs(config: Any, http_path: str, *, oauth_m2m: Callable[[Any], Any]) -> dict:
    """Return kwargs for ``databricks_sql.connect()``.

    Mirrors ``matcha/scripts/databricks_io.py``: scheme-stripped host, the SQL
    warehouse ``http_path``, and a ``credentials_provider`` chosen by auth mode.
    With a service-principal ``client_id`` / ``client_secret`` it uses OAuth M2M;
    otherwise the SDK default (``~/.databrickscfg`` profile).
    """
    if not http_path:
        raise ValueError("DATABRICKS_HTTP_PATH is not set (expected /sql/1.0/warehouses/<id>).")

    if config.client_id and config.client_secret:

        def credentials():
            return oauth_m2m(config)
    else:

        def credentials():
            return config.authenticate

    return {
        "server_hostname": _server_hostname(config.host),
        "http_path": http_path,
        "credentials_provider": credentials,
    }


def _connect_with_retry(
    connect_fn: Callable[..., Any],
    kwargs: dict,
    *,
    max_retries: int = 5,
    retry_delay: int = 10,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Any:
    """Open a connection, retrying on failure to absorb SQL-warehouse cold start.

    Sleeps ``retry_delay`` seconds between attempts; re-raises the last error
    after ``max_retries`` attempts.
    """
    for attempt in range(max_retries):
        try:
            return connect_fn(**kwargs)
        except Exception:
            if attempt == max_retries - 1:
                raise
            sleep_fn(retry_delay)
    raise RuntimeError("unreachable")


def _to_dataframe(description: Sequence[Any], rows: Sequence[Any]) -> pd.DataFrame:
    """Build a DataFrame from a cursor ``description`` and fetched ``rows``.

    Column names come from the first element of each ``description`` entry,
    matching the DB-API shape the sql-connector cursor exposes.
    """
    columns = [col[0] for col in description]
    return pd.DataFrame(list(rows), columns=columns)


def run_query(sql: str, *, max_retries: int = 5, retry_delay: int = 10) -> pd.DataFrame:
    """Execute ``sql`` against the profile's SQL warehouse, return a DataFrame.

    Auth is resolved by the SDK ``Config`` (M2M service principal if configured,
    else the ``~/.databrickscfg`` profile). The SQL warehouse comes from
    ``DATABRICKS_HTTP_PATH`` (``/sql/1.0/warehouses/<id>``).
    """
    from databricks import sql as dbsql
    from databricks.sdk.core import Config, oauth_service_principal

    config = Config()
    kwargs = _build_connect_kwargs(
        config,
        os.environ.get("DATABRICKS_HTTP_PATH", ""),
        oauth_m2m=oauth_service_principal,
    )
    connection = _connect_with_retry(dbsql.connect, kwargs, max_retries=max_retries, retry_delay=retry_delay)
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            return _to_dataframe(cursor.description, rows)
    finally:
        connection.close()
