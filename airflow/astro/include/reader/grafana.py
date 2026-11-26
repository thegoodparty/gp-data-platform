"""Sends the reader's lines to Grafana as OTLP/HTTP logs.

Why the shape is what it is:

- Resource attributes are exactly `service.name` and `deployment.environment.name`.
  Without `service.name` the lines land under `unknown_service` and no Grafana rule
  can select them.
- `pipeline`, `tool` and `status` are log-record attributes, not resource
  attributes: Grafana Cloud stores log-record attributes as structured metadata
  rather than stream labels, so they cost no streams against the 5,000 cap. The
  same fact is recorded for gp-api's own rules in omni's `route-alerts.ts`.
- Every line is stamped at emit time, with the run's own times in the body. A
  replay after downtime stamped at old run times can fall outside the recording
  rule's window, or be rejected as too old.
"""

import base64
import json
import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

import requests

logger = logging.getLogger("airflow.task")

SERVICE_NAME = "data-pipeline-reader"
DEPLOYMENT_ENVIRONMENT = "prod"

# The three log-record attributes. Nothing else leaves the reader as one.
LABEL_KEYS = ("pipeline", "tool", "status")

# Bound on any single body field. The redactor already truncates the error line;
# this is the backstop for a field added later.
BODY_FIELD_LIMIT = 1000


class SendFailed(Exception):
    """The send failed. The task fails, which is what lets rule 4 see the reader go
    quiet, and `save_state` then does not run."""


def _string_value(value: Any) -> dict[str, Any]:
    return {"stringValue": value if isinstance(value, str) else str(value)}


def _body_json(line: Mapping[str, Any]) -> str:
    """The body is JSON of the allowlisted fields, never a log excerpt."""
    trimmed = {
        key: (value[:BODY_FIELD_LIMIT] if isinstance(value, str) else value)
        for key, value in line.items()
        if key not in LABEL_KEYS
    }
    return json.dumps(trimmed, sort_keys=True, separators=(",", ":"))


def build_payload(lines: Iterable[Mapping[str, Any]], emitted_at: datetime) -> dict[str, Any]:
    """One OTLP `resourceLogs` payload for every line."""
    timestamp_ns = str(int(emitted_at.astimezone(UTC).timestamp() * 1_000_000_000))
    records = []
    for line in lines:
        # Exactly three attributes: everything else, times and flags included,
        # stays in the body so it costs no stream and adds no cardinality.
        attributes = [
            {"key": key, "value": _string_value(line.get(key, ""))} for key in LABEL_KEYS if key in line
        ]
        records.append(
            {
                "timeUnixNano": timestamp_ns,
                "observedTimeUnixNano": timestamp_ns,
                # Loki derives the stream's stream field from this, so it is the
                # record type and the pipeline, never a free-text message.
                "severityText": "INFO",
                "body": {"stringValue": _body_json(line)},
                "attributes": attributes,
            }
        )
    return {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": _string_value(SERVICE_NAME)},
                        {
                            "key": "deployment.environment.name",
                            "value": _string_value(DEPLOYMENT_ENVIRONMENT),
                        },
                    ]
                },
                "scopeLogs": [{"scope": {"name": SERVICE_NAME}, "logRecords": records}],
            }
        ]
    }


def send_logs(
    endpoint: str,
    instance_id: str,
    token: str,
    log_lines: Iterable[Mapping[str, Any]],
    timeout: int = 30,
    now: datetime | None = None,
) -> None:
    """POST the lines to the OTLP/HTTP endpoint. Raises SendFailed on any failure."""
    lines = list(log_lines)
    if not lines:
        return
    payload = json.dumps(build_payload(lines, now or datetime.now(UTC))).encode("utf-8")
    credentials = base64.b64encode(f"{instance_id}:{token}".encode()).decode("ascii")
    try:
        response = requests.post(
            endpoint,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Basic {credentials}",
            },
            timeout=timeout,
        )
        response.raise_for_status()
    except Exception as exc:  # any failure must fail the task, so all of them are wrapped
        # The message carries the status code and not the response body, which
        # could quote a line the reader sent.
        raise SendFailed(f"grafana OTLP send failed ({exc.__class__.__name__})") from exc
