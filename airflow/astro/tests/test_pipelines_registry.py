"""The checked-in pipeline list and its loader.

The loader is the CI check for the list: a malformed or incomplete entry has to
fail `pytest`, which fails the airflow workflow, so a typo cannot reach Astro as
a reader that silently watches nothing.
"""

from pathlib import Path

import pytest
from include.pipelines.models import PipelineConfigError
from include.pipelines.registry import load_pipelines, load_pipelines_from

MINIMAL = """
pipelines:
  - pipeline: dbt_nightly_build
    tool: dbt_cloud
    display_name: dbt Cloud nightly production build
    owner: data-engineering
    schedule: "2 */12 * * *"
    allowed_age_hours: 26
    real_success:
      status: run_status_success
      trigger: scheduled
    watched_warnings: []
    depends_on: []
    runbook: https://example.invalid/runbook
    tool_ids:
      job_id: 1234
"""


def _without(text: str, *lines: str) -> str:
    """`text` with the given whole lines dropped: the cheapest way to write a
    YAML entry that is wrong in exactly one way."""
    dropped = set(lines)
    return "\n".join(ln for ln in text.splitlines() if ln not in dropped) + "\n"


def _replacing(text: str, old: str, new: str) -> str:
    assert old in text, old
    return text.replace(old, new)


def test_missing_required_field_is_rejected():
    with pytest.raises(PipelineConfigError) as excinfo:
        load_pipelines(_without(MINIMAL, "    owner: data-engineering"))
    message = str(excinfo.value)
    assert "dbt_nightly_build" in message, message
    assert "owner" in message


def test_nullable_field_must_still_be_present():
    """`schedule: null` is legal (a job with no schedule); omitting the key is not,
    because a missing key is a typo and a null is a decision."""
    assert (
        load_pipelines(_replacing(MINIMAL, 'schedule: "2 */12 * * *"', "schedule: null"))[0].schedule is None
    )


def test_duplicate_pipeline_key_is_rejected():
    with pytest.raises(PipelineConfigError, match="duplicate"):
        load_pipelines(MINIMAL + MINIMAL.split("pipelines:\n", 1)[1])


def test_dbt_cloud_pipeline_requires_job_id():
    with pytest.raises(PipelineConfigError) as excinfo:
        load_pipelines(_without(MINIMAL, "      job_id: 1234"))
    assert "job_id" in str(excinfo.value)


def test_unknown_tool_is_rejected():
    with pytest.raises(PipelineConfigError, match="tool"):
        load_pipelines(_replacing(MINIMAL, "tool: dbt_cloud", "tool: fivetran"))


def test_allowed_age_is_carried_as_seconds():
    """The status update carries a number, so the loader converts once."""
    assert load_pipelines(MINIMAL)[0].allowed_age_seconds == 26 * 3600


def test_unknown_real_success_status_is_rejected():
    with pytest.raises(PipelineConfigError, match="real_success"):
        load_pipelines(_replacing(MINIMAL, "status: run_status_success", "status: looks_green"))


def test_trigger_must_be_one_of_the_known_causes():
    with pytest.raises(PipelineConfigError, match="trigger"):
        load_pipelines(_replacing(MINIMAL, "trigger: scheduled", "trigger: whenever"))


def test_checked_in_list_loads():
    """The real file, not a fixture: this is the check CI runs on a YAML edit."""
    path = Path(__file__).resolve().parents[1] / "include" / "pipelines" / "pipelines.yaml"
    pipelines = load_pipelines_from(path)
    assert [p.pipeline for p in pipelines] == ["dbt_nightly_build"]
    nightly = pipelines[0]
    assert nightly.tool == "dbt_cloud"
    assert nightly.tool_ids.job_id == 70471823431462
    assert nightly.schedule == "2 */12 * * *"
    assert nightly.real_success.status == "run_status_success"
    assert nightly.real_success.trigger == "scheduled"
    assert nightly.allowed_age_seconds == 26 * 3600
    assert nightly.depends_on == ()
