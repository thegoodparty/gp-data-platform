"""Typed form of one entry in the pipeline list.

Frozen because the list is read at DAG parse time and shared by every task in a
run: a collector mutating a pipeline would change what a later task sees.
"""

from dataclasses import dataclass, field

TOOLS = ("dbt_cloud", "airflow", "airbyte")
DBT_CLOUD_STATUSES = ("run_status_success",)
DBT_CLOUD_TRIGGERS = ("scheduled", "on_merge", "manual")

# Every key the YAML entry must carry, even when the value is null or empty.
# A missing key is a typo; a null is a decision, so the two are not the same.
REQUIRED_FIELDS = (
    "pipeline",
    "tool",
    "display_name",
    "owner",
    "schedule",
    "allowed_age_hours",
    "real_success",
    "watched_warnings",
    "depends_on",
    "runbook",
    "tool_ids",
)


class PipelineConfigError(ValueError):
    """A pipeline entry is malformed. The message names the pipeline and field,
    because the person reading it is looking at the YAML."""


@dataclass(frozen=True)
class RealSuccess:
    """What has to happen for a run to count as recovery, per tool."""

    status: str
    trigger: str | None = None


@dataclass(frozen=True)
class ToolIds:
    """Per-tool identifiers. Only dbt Cloud's is used in this PR."""

    job_id: int | None = None


@dataclass(frozen=True)
class Pipeline:
    pipeline: str
    tool: str
    display_name: str
    owner: str
    schedule: str | None
    allowed_age_seconds: int
    real_success: RealSuccess
    runbook: str
    watched_warnings: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    tool_ids: ToolIds = field(default_factory=ToolIds)
