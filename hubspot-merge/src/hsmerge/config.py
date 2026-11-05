"""The job's whole environment surface, read and validated once at startup.

Nothing here reaches an Airflow Variable or Connection: the DAG passes every value
through the environment, so the console script works the same in a pod, in a shell and
under pytest. The Databricks names match what the DAGs already set for reverse-etl
(`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_CLIENT_ID`,
`DATABRICKS_CLIENT_SECRET`, `DATABRICKS_SCOPES`), so the same connection supplies both.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass

SUPPORTED_OBJECT_TYPE = "contacts"

# HubSpot's own default for every private app; named so a shell run needs one variable,
# not two.
DEFAULT_HUBSPOT_BASE_URL = "https://api.hubapi.com"


class MissingConfigError(ValueError):
    """A required variable is absent, unparseable, or carries a rejected value.

    One error for all three, because they are one failure: the environment cannot
    produce a usable run. `env_var` names the variable so the operator knows what to set.
    """

    def __init__(self, env_var: str, reason: str):
        self.env_var = env_var
        self.reason = reason
        super().__init__(f"{env_var}: {reason}")


@dataclass(frozen=True)
class Config:
    hubspot_token: str
    hubspot_base_url: str
    expected_portal_id: str
    object_type: str
    pair_relation: str
    log_table: str
    max_pairs: int
    max_rps: float
    databricks_host: str
    databricks_http_path: str
    databricks_client_id: str
    databricks_client_secret: str
    databricks_scopes: str = ""

    def __repr__(self) -> str:
        return f"Config(hubspot_base_url={self.hubspot_base_url!r}, object_type={self.object_type!r})"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        """Build the config from `env`, or raise `MissingConfigError` naming what is wrong."""

        def require(name: str) -> str:
            value = env.get(name, "").strip()
            if not value:
                raise MissingConfigError(name, "is required and unset")
            return value

        object_type = require("MERGE_OBJECT_TYPE")
        if object_type != SUPPORTED_OBJECT_TYPE:
            raise MissingConfigError(
                "MERGE_OBJECT_TYPE",
                f"must be {SUPPORTED_OBJECT_TYPE!r} until another object type is supported",
            )

        return cls(
            hubspot_token=require("HUBSPOT_TOKEN"),
            hubspot_base_url=env.get("HUBSPOT_BASE_URL", "").strip() or DEFAULT_HUBSPOT_BASE_URL,
            expected_portal_id=require("HUBSPOT_EXPECTED_PORTAL_ID"),
            object_type=object_type,
            pair_relation=require("MERGE_PAIR_RELATION"),
            log_table=require("MERGE_LOG_TABLE"),
            max_pairs=_positive_int(env, "MERGE_MAX_PAIRS"),
            max_rps=_positive_float(env, "MERGE_MAX_RPS"),
            databricks_host=require("DATABRICKS_HOST"),
            databricks_http_path=require("DATABRICKS_HTTP_PATH"),
            databricks_client_id=require("DATABRICKS_CLIENT_ID"),
            databricks_client_secret=require("DATABRICKS_CLIENT_SECRET"),
            databricks_scopes=env.get("DATABRICKS_SCOPES", "").strip(),
        )


def _positive_int(env: Mapping[str, str], name: str) -> int:
    raw = env.get(name, "").strip()
    if not raw:
        raise MissingConfigError(name, "is required and unset")
    try:
        value = int(raw)
    except ValueError as exc:
        raise MissingConfigError(name, f"must be a whole number, got {raw!r}") from exc
    if value <= 0:
        # A cap of zero would merge nothing forever while the run reports success.
        raise MissingConfigError(name, f"must be positive, got {value}")
    return value


def _positive_float(env: Mapping[str, str], name: str) -> float:
    raw = env.get(name, "").strip()
    if not raw:
        raise MissingConfigError(name, "is required and unset")
    try:
        value = float(raw)
    except ValueError as exc:
        raise MissingConfigError(name, f"must be a number, got {raw!r}") from exc
    if value <= 0 or not math.isfinite(value):
        raise MissingConfigError(name, f"must be positive, got {value}")
    return value
