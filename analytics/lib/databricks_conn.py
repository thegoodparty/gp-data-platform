"""Build a ``run_query`` callable from Databricks CLI credentials.

For contributors authenticated via ``databricks auth login`` (OAuth, profiles in
``~/.databrickscfg`` with ``auth_type = databricks-cli`` and the token cached under
``~/.databricks/``) rather than the ``DATABRICKS_*`` env vars. ``win_analysis`` is
connection-agnostic; this just produces the ``run_query(sql) -> DataFrame`` it expects.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd
from databricks import sql
from databricks.sdk.core import Config

# Serverless Starter Warehouse (the always-on one). Override per call if needed.
DEFAULT_WAREHOUSE_ID = "18583d8b081c6486"


def make_run_query(
    profile: str = "DEFAULT",
    warehouse_id: str = DEFAULT_WAREHOUSE_ID,
) -> Callable[[str], pd.DataFrame]:
    """Return a ``run_query(sql) -> DataFrame`` backed by CLI OAuth credentials.

    Args:
        profile: ``~/.databrickscfg`` profile to resolve (host + OAuth).
        warehouse_id: SQL warehouse to run against.

    Resolves the cached OAuth token via the SDK ``Config``. If it has expired, run
    ``databricks auth login --host <host>`` to refresh, then retry.
    """
    cfg = Config(profile=profile)
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    hostname = cfg.host.replace("https://", "")

    def run_query(query: str) -> pd.DataFrame:
        # Resolve a fresh bearer token from the cached OAuth credentials each call,
        # so a long-lived run_query survives token rotation.
        access_token = cfg.oauth_token().access_token
        with (
            sql.connect(
                server_hostname=hostname,
                http_path=http_path,
                access_token=access_token,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(query)
            return cur.fetchall_arrow().to_pandas()

    return run_query
