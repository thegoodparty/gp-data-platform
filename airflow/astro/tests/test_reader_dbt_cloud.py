"""The dbt Cloud client against canned provider responses.

No live call: the hook is a fake, and the fixtures are shaped from dbt Cloud's
documented Admin API response, with the fields the reader reads and no others.
The step parameter has its own test because getting it wrong is silent: the
artifact of the wrong step looks like a green run.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from include.reader.dbt_cloud import (
    CELL_HOST,
    PAGE_LIMIT,
    ArtifactUnavailable,
    CellHostMismatch,
    DbtCloudClient,
)


def _run(
    run_id: int,
    status: int = 10,
    finished: str | None = "2026-11-18T02:40:00Z",
    created: str = "2026-11-18T00:02:00Z",
    started: str | None = "2026-11-18T00:02:30Z",
    cause: str = "Scheduled",
) -> dict:
    """One entry of the Admin API's `data` list, as documented."""
    return {
        "id": run_id,
        "job_definition_id": 70471823431462,
        "status": status,
        "created_at": created,
        "started_at": started,
        "finished_at": finished,
        "href": f"https://{CELL_HOST}/deploy/70471823431462/runs/{run_id}/",
        "trigger": {"cause": cause},
    }


def _hook(
    pages: list[list[dict]] | None = None, artifact: dict | None = None, steps: list[dict] | None = None
):
    hook = MagicMock()
    pages = pages or [[]]
    calls: list[dict] = []

    def get_job_runs(payload=None):
        calls.append(dict(payload or {}))
        offset = (payload or {}).get("offset", 0)
        limit = (payload or {}).get("limit", PAGE_LIMIT)
        index = offset // limit
        response = MagicMock()
        response.json.return_value = {"data": pages[index] if index < len(pages) else []}
        return response

    hook.get_job_runs.side_effect = get_job_runs
    hook.get_job_runs.calls = calls
    run_response = MagicMock()
    run_response.json.return_value = {"data": {"run_steps": steps if steps is not None else []}}
    hook.get_job_run.return_value = run_response
    if artifact is not None:
        artifact_response = MagicMock()
        # No `data` wrapper: the artifact endpoint returns the file itself. See
        # _artifact() below for the shape this stands in for.
        artifact_response.json.return_value = artifact
        hook.get_job_run_artifact.return_value = artifact_response
    else:
        hook.get_job_run_artifact.side_effect = RuntimeError("404")
    return hook


def _artifact(*results: dict) -> dict:
    """`run_results.json` in its documented shape: a top-level `results` list, each
    result carrying unique_id, status and message. Shaped from the documented dbt
    artifact format, not from a live call, and carrying no production text."""
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
        "args": {},
        "elapsed_time": 0.0,
        "results": list(results),
    }


ARTIFACT_WITH_A_FAILURE = _artifact(
    {"unique_id": "model.pkg.stg_x", "status": "success", "message": ""},
    {
        "unique_id": "test.pkg.assert_x",
        "status": "fail",
        "message": "Got 1 result, configured to fail if != 0",
    },
)


def test_fetch_run_results_reads_the_artifact_file_not_a_data_wrapper():
    """The artifact endpoint returns the file itself. A `data` wrapper would raise
    KeyError here, which the collector swallows, so every record would be
    status-only and this is the test that keeps that from being silent."""
    client = DbtCloudClient(_hook(artifact=ARTIFACT_WITH_A_FAILURE))
    body = client.fetch_run_results(900, step=2)
    assert body["results"][1]["unique_id"] == "test.pkg.assert_x"
    assert body["results"][1]["message"]


def test_a_wrapped_body_is_rejected_rather_than_read_as_empty():
    """A payload with a top-level `data` key and no `results` must not become an
    empty artifact: an empty artifact reads exactly like a run where nothing failed."""
    client = DbtCloudClient(_hook(artifact={"data": ARTIFACT_WITH_A_FAILURE}))
    with pytest.raises(ArtifactUnavailable):
        client.fetch_run_results(900, step=2)


def test_a_body_with_no_results_list_is_rejected():
    for body in ({"metadata": {}}, [], "not json", {}):
        client = DbtCloudClient(_hook(artifact=body))
        with pytest.raises(ArtifactUnavailable):
            client.fetch_run_results(900, step=2)


def test_run_results_url_carries_the_step_parameter():
    """Without the step, the artifact is the LAST step's: for a multi-step job
    that is often docs generate, which is green while the build is red."""
    client = DbtCloudClient(_hook(artifact=_artifact()))
    client.fetch_run_results(900, step=3)
    assert client._hook.get_job_run_artifact.call_args.kwargs["step"] == 3


def test_a_call_without_a_step_is_impossible():
    client = DbtCloudClient(_hook(artifact=_artifact()))
    with pytest.raises(TypeError):
        client.fetch_run_results(900)  # type: ignore[call-arg]
    with pytest.raises(ValueError):
        client.fetch_run_results(900, step=0)


def test_list_runs_filters_on_finish_time_not_creation_time():
    """A long run finishing after a later-created one must still be seen."""
    older = _run(2, finished="2026-11-18T03:10:00Z", created="2026-11-18T00:02:00Z")
    newer = _run(3, finished="2026-11-18T02:00:00Z", created="2026-11-18T02:02:00Z")
    client = DbtCloudClient(_hook([[newer, older]]))
    cutoff = datetime(2026, 11, 18, 1, 0, tzinfo=UTC)
    assert {r.run_id for r in client.list_runs(70471823431462, cutoff)} == {2, 3}


def test_an_unfinished_run_is_not_returned_as_finished():
    client = DbtCloudClient(_hook([[_run(5, status=3, finished=None, started="2026-11-18T00:02:30Z")]]))
    assert client.list_runs(70471823431462) == []


def test_first_unfinished_finds_the_started_run():
    """A started-but-unfinished run still counts as started for the due time."""
    client = DbtCloudClient(_hook([[_run(6, status=3, finished=None)]]))
    run = client.first_unfinished(70471823431462)
    assert run is not None and run.run_id == 6


def test_pagination_is_followed():
    full_page = [_run(1000 + i) for i in range(PAGE_LIMIT)]
    second_page = [_run(500), _run(499)]
    client = DbtCloudClient(_hook([full_page, second_page]))
    runs = client.list_runs(70471823431462)
    assert len(runs) == PAGE_LIMIT + 2


def test_pagination_stops_on_a_short_page():
    client = DbtCloudClient(_hook([[_run(1), _run(2)]]))
    client.list_runs(70471823431462)
    assert len(client._hook.get_job_runs.calls) == 1


def test_cell_host_is_enforced():
    """The generic host answers the Admin API with a confusing 404, so the
    mismatch is caught before the first call instead."""
    hook = _hook()
    hook.get_connection.return_value.host = "cloud.getdbt.com"
    with pytest.raises(CellHostMismatch):
        DbtCloudClient(hook).check_host()


def test_the_cell_host_is_accepted():
    hook = _hook()
    hook.get_connection.return_value.host = CELL_HOST
    DbtCloudClient(hook).check_host()


def test_run_shape_fixture_matches_the_api():
    """The fields the collector relies on, from dbt Cloud's documented response
    shape. The trigger's cause is the one that marks a scheduled run."""
    run = DbtCloudClient(_hook([[_run(9, cause="Scheduled")]])).list_runs(70471823431462)[0]
    assert run.scheduled is True
    assert run.outcome == "success"
    assert run.finished_at == datetime(2026, 11, 18, 2, 40, tzinfo=UTC)


def test_a_manual_run_is_not_scheduled():
    run = DbtCloudClient(_hook([[_run(9, cause="Manual")]])).list_runs(70471823431462)[0]
    assert run.scheduled is False


def test_missing_artifact_raises_artifact_unavailable():
    client = DbtCloudClient(_hook(artifact=None))
    with pytest.raises(ArtifactUnavailable):
        client.fetch_run_results(900, step=1)


def test_list_steps_carries_the_index():
    client = DbtCloudClient(_hook(steps=[{"index": 1, "name": "Invoke dbt with `dbt build`", "status": 10}]))
    steps = client.list_steps(900)
    assert [(s.index, s.name) for s in steps] == [(1, "Invoke dbt with `dbt build`")]


def test_the_run_endpoint_wraps_its_object_in_data():
    """The other shape: the run endpoint (unlike the artifact one) returns
    `{"data": {...}}`, which is how the provider's own OpenLineage util reads
    run_steps. A GET of the run is asked for with run_steps related."""
    client = DbtCloudClient(_hook(steps=[{"index": 2, "name": "x", "status": 10}]))
    client.list_steps(900)
    assert client._hook.get_job_run.call_args.kwargs["include_related"] == ["run_steps"]


def test_a_run_response_without_the_data_wrapper_yields_no_steps():
    """If the run endpoint ever stopped wrapping, list_steps returns nothing rather
    than inventing a step; the collector then degrades to a status-only record."""
    hook = _hook()
    hook.get_job_run.return_value.json.return_value = {"run_steps": [{"index": 1, "name": "x", "status": 10}]}
    with pytest.raises(KeyError):
        DbtCloudClient(hook).list_steps(900)


def test_a_trigger_cause_is_matched_on_its_stem():
    """The exact string varies by how a run was started, so equality would classify
    the nightly as manual and it could never count as recovery."""
    for cause, scheduled in (
        ("Scheduled", True),
        ("Scheduled by timer", True),
        ("Manual", False),
        ("Kicked off from the UI", False),
        ("", False),
    ):
        run = DbtCloudClient(_hook([[_run(9, cause=cause)]])).list_runs(70471823431462)[0]
        assert run.scheduled is scheduled, cause


def test_paging_stops_once_creation_is_well_before_the_cutoff():
    """Without a stop a cold start walks the job's whole history, one request per
    hundred runs; the stop has to allow for a run created before the window that
    finished inside it, which is why it is a span and not the cutoff itself."""
    old = [
        _run(1000 - i, created="2026-11-01T00:02:00Z", finished="2026-11-01T02:00:00Z")
        for i in range(PAGE_LIMIT)
    ]
    new = [_run(50, created="2026-11-18T00:02:00Z", finished="2026-11-18T02:00:00Z")]
    # A full first page is what makes the client ask for a second one at all.
    new += [
        _run(40 - i, created="2026-11-18T00:02:00Z", finished="2026-11-18T02:00:00Z")
        for i in range(PAGE_LIMIT - 1)
    ]
    client = DbtCloudClient(_hook([new, old]))
    cutoff = datetime(2026, 11, 18, 1, 0, tzinfo=UTC)
    runs = client.list_runs(70471823431462, cutoff)
    assert len(runs) == PAGE_LIMIT
    assert 1000 - 0 not in [r.run_id for r in runs]
    # Two requests: the full first page, then the page that stopped it.
    assert len(client._hook.get_job_runs.calls) == 2


def test_a_long_run_created_before_the_window_is_still_collected():
    long_run = _run(7, created="2026-11-17T20:02:00Z", finished="2026-11-18T02:00:00Z")
    client = DbtCloudClient(_hook([[long_run]]))
    cutoff = datetime(2026, 11, 18, 1, 0, tzinfo=UTC)
    assert [r.run_id for r in client.list_runs(70471823431462, cutoff)] == [7]


def test_first_unfinished_reads_only_the_first_page():
    client = DbtCloudClient(_hook([[_run(9), _run(8, status=3, finished=None)]]))
    assert client.first_unfinished(70471823431462).run_id == 8
    assert len(client._hook.get_job_runs.calls) == 1
