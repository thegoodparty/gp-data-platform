"""Loader configuration, read from the environment.

Standalone (not a `loader.core` subclass): this loader has no S3/RDS/manifest
concerns, so pulling in people-api-loader's harness would bring dependencies
this pipeline doesn't need.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(slots=True, kw_only=True)
class Config:
    anthropic_analytics_api_key: str
    databricks_warehouse_id: str
    schema: str
    catalog: str = "goodparty_data_catalog"

    @classmethod
    def from_env(cls) -> Config:
        key = os.environ.get("ANTHROPIC_ANALYTICS_API_KEY")
        if not key:
            raise RuntimeError("ANTHROPIC_ANALYTICS_API_KEY is not set.")
        warehouse_id = os.environ.get("LOADER_DATABRICKS_WAREHOUSE_ID")
        if not warehouse_id:
            raise RuntimeError("LOADER_DATABRICKS_WAREHOUSE_ID is not set.")
        # No default: dbt_audrey is a personal dev schema, not a safe fallback for a scheduled
        # production sync. Every caller (DAG env, local .env) must set this explicitly.
        schema = os.environ.get("LOADER_SCHEMA")
        if not schema:
            raise RuntimeError("LOADER_SCHEMA is not set.")
        return cls(
            anthropic_analytics_api_key=key,
            databricks_warehouse_id=warehouse_id,
            schema=schema,
            catalog=os.environ.get("LOADER_CATALOG", "goodparty_data_catalog"),
        )
