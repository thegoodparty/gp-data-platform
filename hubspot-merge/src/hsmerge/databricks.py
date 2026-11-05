"""Opens the warehouse connection the reader and the log share."""

from __future__ import annotations

from typing import Any

from .config import Config


def connect(config: Config) -> Any:
    """The SDK resolves credentials: the service principal when its id and secret are set,
    otherwise whatever the environment offers, such as a `databricks auth login` session."""
    import databricks.sql as databricks_sql
    from databricks.sdk.core import Config as SdkConfig

    sdk_config = SdkConfig(
        host=config.databricks_host,
        client_id=config.databricks_client_id or None,
        client_secret=config.databricks_client_secret or None,
        scopes=config.databricks_scopes or None,
    )
    return databricks_sql.connect(
        server_hostname=config.databricks_host.removeprefix("https://").rstrip("/"),
        http_path=config.databricks_http_path,
        credentials_provider=lambda: sdk_config.authenticate,
    )
