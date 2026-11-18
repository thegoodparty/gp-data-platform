"""The OTLP payload's shape.

Ticket 5's recording rules select on these names, so they are pinned here. The
tests read the payload the sender actually builds, not a helper's return value.
"""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from include.reader.grafana import (
    DEPLOYMENT_ENVIRONMENT,
    LABEL_KEYS,
    SERVICE_NAME,
    SendFailed,
    build_payload,
    send_logs,
)

AT = datetime(2026, 11, 18, 6, 0, tzinfo=UTC)

LINE = {
    "record_type": "status_update",
    "pipeline": "dbt_nightly_build",
    "tool": "dbt_cloud",
    "status": "error",
    "pipeline_x": "ignored",
}


def _resource_attributes(payload):
    attrs = payload["resourceLogs"][0]["resource"]["attributes"]
    return {a["key"]: a["value"]["stringValue"] for a in attrs}


def _record(payload):
    return payload["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]


def test_resource_attributes_are_exactly_service_and_environment():
    """Without service.name the lines land under unknown_service and no rule can
    find them; anything high-cardinality on the resource would mint a stream per
    value against the 5,000 cap."""
    attrs = _resource_attributes(build_payload([LINE], AT))
    assert attrs == {
        "service.name": SERVICE_NAME,
        "deployment.environment.name": DEPLOYMENT_ENVIRONMENT,
    }
    assert SERVICE_NAME == "data-pipeline-reader"
    assert DEPLOYMENT_ENVIRONMENT == "prod"


def test_pipeline_tool_and_status_are_log_record_attributes_not_resource_ones():
    payload = build_payload([LINE], AT)
    assert set(_resource_attributes(payload)) == {"service.name", "deployment.environment.name"}
    record_attrs = {a["key"] for a in _record(payload)["attributes"]}
    assert {"pipeline", "tool", "status"} <= record_attrs


def test_exactly_three_labels_leave_the_reader():
    payload = build_payload([LINE], AT)
    record_attrs = {a["key"] for a in _record(payload)["attributes"]}
    # pipeline_x is not a label: only the three named keys are lifted out of the body.
    assert record_attrs == set(LABEL_KEYS)
    # And the body still carries everything else, so nothing is lost.
    body = json.loads(_record(payload)["body"]["stringValue"])
    assert body["pipeline_x"] == "ignored"
    assert "pipeline" not in body


def test_every_line_is_stamped_at_emit_time():
    """A replay after downtime must not stamp old run times: the run's own times
    are in the body, where a rule reads them as values."""
    old_run = {
        "pipeline": "dbt_nightly_build",
        "tool": "dbt_cloud",
        "status": "error",
        "finished_at": 1_760_000_000,
    }
    payload = build_payload([old_run], AT)
    record = _record(payload)
    assert record["timeUnixNano"] == str(int(AT.timestamp() * 1_000_000_000))
    body = json.loads(record["body"]["stringValue"])
    assert body["finished_at"] == 1_760_000_000


def test_body_is_json_and_has_no_log_text():
    long_value = "x" * 5000
    payload = build_payload([{**LINE, "error": long_value}], AT)
    body = json.loads(_record(payload)["body"]["stringValue"])
    assert len(body["error"]) <= 1000
    for key in body:
        assert "log" not in key.lower()
        assert "excerpt" not in key.lower()


def test_a_flag_and_a_time_survive_as_json_numbers():
    """A recording rule unwraps a number and cannot unwrap a quoted string, so the
    body has to carry these as JSON numbers and not as strings."""
    payload = build_payload([{**LINE, "failed_since_last_success": 1, "next_run_due_at": 1_760_000_000}], AT)
    body = json.loads(_record(payload)["body"]["stringValue"])
    assert body["failed_since_last_success"] == 1
    assert body["next_run_due_at"] == 1_760_000_000
    # And neither of them became a label, which would mint a stream per value.
    assert {a["key"] for a in _record(payload)["attributes"]} == set(LABEL_KEYS)


def test_no_lines_sends_nothing():
    with patch("include.reader.grafana.requests.post") as post:
        send_logs("https://example.invalid", "1", "token", [])
    assert not post.called


def test_send_posts_basic_auth_and_the_payload():
    response = MagicMock()
    with patch("include.reader.grafana.requests.post", return_value=response) as post:
        send_logs("https://example.invalid/otlp/v1/logs", "123", "secret", [LINE], timeout=5, now=AT)
    url = post.call_args.args[0]
    kwargs = post.call_args.kwargs
    assert url == "https://example.invalid/otlp/v1/logs"
    assert kwargs["timeout"] == 5
    assert kwargs["headers"]["Content-Type"] == "application/json"
    assert kwargs["headers"]["Authorization"].startswith("Basic ")
    payload = json.loads(kwargs["data"])
    assert _resource_attributes(payload)["service.name"] == SERVICE_NAME


def test_send_failure_raises_so_the_task_fails():
    """A Grafana failure has to fail the reader run: that is what makes rule 4 able
    to see the reader go quiet. save_state then does not run."""
    with (
        patch("include.reader.grafana.requests.post", side_effect=OSError("down")),
        pytest.raises(SendFailed),
    ):
        send_logs("https://example.invalid", "1", "token", [LINE], now=AT)


def test_a_rejected_status_raises_too():
    response = MagicMock()
    response.raise_for_status.side_effect = __import__("requests").HTTPError("401")
    with (
        patch("include.reader.grafana.requests.post", return_value=response),
        pytest.raises(SendFailed),
    ):
        send_logs("https://example.invalid", "1", "token", [LINE], now=AT)
