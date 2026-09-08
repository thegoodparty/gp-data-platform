"""Environment-driven flow and run configuration.

Structural facts about a flow (the model, its key column, its excluded columns, its
send cap) come from environment variables namespaced by the flow's own name, so
adding a flow is a deployment change, never a code change; there is no registry of
known flow names to extend. The source model's relation name is config, never a
hardcoded string, for the same reason: this package is built before the model it
will read exists.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


class MissingFlowConfigError(ValueError):
    def __init__(self, flow_id: str, env_var: str):
        self.flow_id = flow_id
        self.env_var = env_var
        super().__init__(f"flow {flow_id!r} is missing required environment variable {env_var}")


@dataclass(frozen=True)
class FlowConfig:
    flow_id: str
    source_relation: str
    key_column: str
    excluded_columns: frozenset[str]
    cap: int  # no code default: sizing this is an enable-time, per-flow call


def load_flow_config(flow_id: str, env: Mapping[str, str]) -> FlowConfig:
    """Build `flow_id`'s config from its `RETL_FLOW_<FLOW_ID>_*` environment variables."""
    prefix = f"RETL_FLOW_{flow_id.upper()}_"

    def require(name: str) -> str:
        value = env.get(prefix + name, "")
        if not value:
            raise MissingFlowConfigError(flow_id, prefix + name)
        return value

    excluded_raw = env.get(prefix + "EXCLUDED_COLUMNS", "")
    excluded_columns = frozenset(c.strip() for c in excluded_raw.split(",") if c.strip())

    cap_raw = require("CAP")
    try:
        cap = int(cap_raw)
    except ValueError as exc:
        raise MissingFlowConfigError(flow_id, prefix + "CAP") from exc

    return FlowConfig(
        flow_id=flow_id,
        source_relation=require("SOURCE_RELATION"),
        key_column=require("KEY_COLUMN"),
        excluded_columns=excluded_columns,
        cap=cap,
    )


@dataclass(frozen=True)
class RunEnvConfig:
    """Facts shared by every flow in one run: which sent_log-shaped table this run uses.

    One value, read once, regardless of destination: a CSV preview reads the same
    table (to diff against what is really logged) but never writes to it; a sandbox
    run points this at a scratch-schema table instead of the production one.
    """

    log_table: str


def load_run_env_config(env: Mapping[str, str]) -> RunEnvConfig:
    log_table = env.get("RETL_LOG_TABLE", "")
    if not log_table:
        raise ValueError("RETL_LOG_TABLE is not set")
    return RunEnvConfig(log_table=log_table)
