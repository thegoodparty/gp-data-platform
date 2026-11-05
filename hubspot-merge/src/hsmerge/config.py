"""The job's environment surface, read and validated at startup.

Nothing here reaches an Airflow Variable or Connection: the DAG passes every value through
the environment. The Databricks names are the ones the reverse-etl DAGs already set, so one
connection supplies both jobs.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass

SUPPORTED_OBJECT_TYPE = "contacts"

DEFAULT_HUBSPOT_BASE_URL = "https://api.hubapi.com"

_TABLE_NAME = re.compile(r"\w+\.\w+\.\w+")


class MissingConfigError(ValueError):
    """A required variable is absent, unparseable, or carries a rejected value."""

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
    # Unset, the Databricks SDK finds credentials itself, such as a CLI login for a
    # hand run. The DAG always sets them.
    databricks_client_id: str = ""
    databricks_client_secret: str = ""
    databricks_scopes: str = ""

    def __repr__(self) -> str:
        return f"Config(hubspot_base_url={self.hubspot_base_url!r}, object_type={self.object_type!r})"

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Config:
        object_type = _require(env, "MERGE_OBJECT_TYPE")
        if object_type != SUPPORTED_OBJECT_TYPE:
            raise MissingConfigError(
                "MERGE_OBJECT_TYPE",
                f"must be {SUPPORTED_OBJECT_TYPE!r} until another object type is supported",
            )

        return cls(
            hubspot_token=_require(env, "HUBSPOT_TOKEN"),
            hubspot_base_url=env.get("HUBSPOT_BASE_URL", "").strip() or DEFAULT_HUBSPOT_BASE_URL,
            expected_portal_id=_require(env, "HUBSPOT_EXPECTED_PORTAL_ID"),
            object_type=object_type,
            pair_relation=_table_name(env, "MERGE_PAIR_RELATION"),
            log_table=_table_name(env, "MERGE_LOG_TABLE"),
            max_pairs=_positive_int(env, "MERGE_MAX_PAIRS"),
            max_rps=_positive_float(env, "MERGE_MAX_RPS"),
            databricks_host=_require(env, "DATABRICKS_HOST"),
            databricks_http_path=_require(env, "DATABRICKS_HTTP_PATH"),
            databricks_client_id=env.get("DATABRICKS_CLIENT_ID", "").strip(),
            databricks_client_secret=env.get("DATABRICKS_CLIENT_SECRET", "").strip(),
            databricks_scopes=env.get("DATABRICKS_SCOPES", "").strip(),
        )


def _require(env: Mapping[str, str], name: str) -> str:
    value = env.get(name, "").strip()
    if not value:
        raise MissingConfigError(name, "is required and unset")
    return value


def _table_name(env: Mapping[str, str], name: str) -> str:
    # Interpolated into SQL, and a DAG trigger param can set it.
    value = _require(env, name)
    if not _TABLE_NAME.fullmatch(value):
        raise MissingConfigError(name, f"must be catalog.schema.table, got {value!r}")
    return value


def _positive_int(env: Mapping[str, str], name: str) -> int:
    raw = _require(env, name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise MissingConfigError(name, f"must be a whole number, got {raw!r}") from exc
    if value <= 0:
        # A cap of zero merges nothing forever while the run reports success.
        raise MissingConfigError(name, f"must be positive, got {value}")
    return value


def _positive_float(env: Mapping[str, str], name: str) -> float:
    raw = _require(env, name)
    try:
        value = float(raw)
    except ValueError as exc:
        raise MissingConfigError(name, f"must be a number, got {raw!r}") from exc
    # nan passes every positive comparison, so it needs its own check.
    if value <= 0 or not math.isfinite(value):
        raise MissingConfigError(name, f"must be positive, got {value}")
    return value
