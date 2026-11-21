"""Loads and validates the checked-in pipeline list.

Read at task time, so it must not touch the network or the metastore: this module
parses YAML and nothing else. The real check on the list is the test that loads the
checked-in file, which fails `pytest` and so the airflow workflow.

PyYAML is imported here at DAG run time, so it has to exist on the Astro image and
not only in the dev group. It does, transitively: apache-airflow-core declares
pyyaml>=6.0.3 as a hard requirement, and both the image and `airflow/pyproject.toml`
pin apache-airflow==3.3.2. It is deliberately not duplicated into requirements.txt,
where a second pin could drift from Airflow's own.
"""

from pathlib import Path
from typing import Any

import yaml
from include.pipelines.models import (
    DBT_CLOUD_STATUSES,
    DBT_CLOUD_TRIGGERS,
    REQUIRED_FIELDS,
    TOOLS,
    Pipeline,
    PipelineConfigError,
    RealSuccess,
    ToolIds,
)


def _fail(name: str, detail: str) -> PipelineConfigError:
    return PipelineConfigError(f"pipeline {name!r}: {detail}")


def _one(entry: Any, name_for_errors: str) -> Pipeline:
    if not isinstance(entry, dict):
        raise PipelineConfigError(f"pipeline entry {name_for_errors!r} is not a mapping")
    name = str(entry.get("pipeline") or name_for_errors)
    for field in REQUIRED_FIELDS:
        if field not in entry:
            raise _fail(name, f"missing required field {field!r}")
    for field in ("pipeline", "display_name", "owner", "runbook"):
        if not isinstance(entry[field], str) or not entry[field].strip():
            raise _fail(name, f"{field!r} must be a non-empty string")

    tool = entry["tool"]
    if tool not in TOOLS:
        raise _fail(name, f"unknown tool {tool!r}; expected one of {', '.join(TOOLS)}")

    schedule = entry["schedule"]
    if schedule is not None and not isinstance(schedule, str):
        raise _fail(name, "'schedule' must be a cron string or null")

    age = entry["allowed_age_hours"]
    if not isinstance(age, int | float) or isinstance(age, bool) or age <= 0:
        raise _fail(name, "'allowed_age_hours' must be a positive number")

    watch = entry["watched_warnings"]
    depends = entry["depends_on"]
    for field, value in (("watched_warnings", watch), ("depends_on", depends)):
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise _fail(name, f"{field!r} must be a list of strings")

    real_success = entry["real_success"]
    if not isinstance(real_success, dict):
        raise _fail(name, "'real_success' must be a mapping")
    if tool == "dbt_cloud":
        status = real_success.get("status")
        if status not in DBT_CLOUD_STATUSES:
            raise _fail(
                name,
                f"real_success.status {status!r} is not one of {', '.join(DBT_CLOUD_STATUSES)}",
            )
        trigger = real_success.get("trigger")
        if trigger is not None and trigger not in DBT_CLOUD_TRIGGERS:
            raise _fail(
                name,
                f"real_success.trigger {trigger!r} is not one of {', '.join(DBT_CLOUD_TRIGGERS)}",
            )
    elif "status" not in real_success and "task" not in real_success:
        raise _fail(name, "'real_success' needs a 'status' or a 'task'")

    tool_ids = entry["tool_ids"]
    # An empty block parses as None; treat it as an empty mapping so the error
    # names the key the tool actually needs rather than the container.
    if tool_ids is None:
        tool_ids = {}
    if not isinstance(tool_ids, dict):
        raise _fail(name, "'tool_ids' must be a mapping")
    job_id = tool_ids.get("job_id")
    if tool == "dbt_cloud":
        if job_id is None:
            raise _fail(name, "a dbt_cloud pipeline requires tool_ids.job_id")
        if not isinstance(job_id, int) or isinstance(job_id, bool):
            raise _fail(name, "'tool_ids.job_id' must be an integer")

    return Pipeline(
        pipeline=entry["pipeline"],
        tool=tool,
        display_name=entry["display_name"],
        owner=entry["owner"],
        schedule=schedule,
        # Seconds, because the status update has to carry a number: a recording
        # rule can unwrap one and cannot unwrap an ISO string.
        allowed_age_seconds=round(age * 3600),
        real_success=RealSuccess(
            status=str(real_success.get("status") or ""),
            trigger=real_success.get("trigger"),
        ),
        runbook=entry["runbook"],
        watched_warnings=tuple(watch),
        depends_on=tuple(depends),
        tool_ids=ToolIds(
            job_id=job_id if isinstance(job_id, int) else None,
        ),
    )


def load_pipelines(text: str) -> list[Pipeline]:
    """Parse and validate the list. Raises PipelineConfigError on the first bad entry."""
    document = yaml.safe_load(text)
    if not isinstance(document, dict) or "pipelines" not in document:
        raise PipelineConfigError("the pipeline list must be a mapping with a 'pipelines' key")
    entries = document["pipelines"]
    if not isinstance(entries, list) or not entries:
        raise PipelineConfigError("'pipelines' must be a non-empty list")

    seen: set[str] = set()
    pipelines: list[Pipeline] = []
    for index, entry in enumerate(entries):
        pipeline = _one(entry, f"entry {index}")
        if pipeline.pipeline in seen:
            raise PipelineConfigError(f"duplicate pipeline key {pipeline.pipeline!r}")
        seen.add(pipeline.pipeline)
        pipelines.append(pipeline)

    known = {p.pipeline for p in pipelines}
    for pipeline in pipelines:
        for upstream in pipeline.depends_on:
            if upstream not in known:
                raise _fail(pipeline.pipeline, f"depends_on names unknown pipeline {upstream!r}")
    return pipelines


def load_pipelines_from(path: Path) -> list[Pipeline]:
    return load_pipelines(path.read_text(encoding="utf-8"))
