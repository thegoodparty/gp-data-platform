"""Tests for the BallotReady filing-period fetcher's API handling.

Races reference filing periods by id only, so a dropped id costs us the filing
deadline for every race pointing at it, silently. Two properties are pinned here
because both were previously violated in ways that only showed up as coverage
decay: a node the API cannot resolve must not cost its 200-id batch, and a
transient HTTP failure must be retried rather than abandoned. A third pins the
normalized record against its declared Spark schema, since a field whose pandas
dtype cannot reach its Spark type would fail only on the cluster.

The module is imported by path (it lives under project/models/, not on the
package path) and only its pure helpers are exercised — the pandas UDF and
model() need a Spark session and Databricks secrets.
"""

import importlib.util
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import requests

MODEL_PATH = (
    Path(__file__).parent.parent
    / "project"
    / "models"
    / "intermediate"
    / "ballotready_api"
    / "int__ballotready_filing_period.py"
)


def _load_model_module():
    spec = importlib.util.spec_from_file_location("int__ballotready_filing_period", MODEL_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = _load_model_module()


def _node(**overrides: Any) -> dict[str, Any]:
    node = {
        "createdAt": "2025-01-02T03:04:05Z",
        "databaseId": 12345,
        "endOn": "2026-03-06",
        "id": "gid://ballot-factory/FilingPeriod/12345",
        "notes": None,
        "startOn": "2026-01-02",
        "type": "Standard",
        "updatedAt": "2025-06-07T08:09:10Z",
    }
    node.update(overrides)
    return node


class _FakeResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        # `response=self` matters: the handler routes on `e.response.status_code`, so a
        # response-less HTTPError would test the None fallback rather than the status.
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"status {self.status_code}",
                response=self,  # type: ignore[arg-type]
            )


@pytest.fixture(autouse=True)
def _no_sleeping(monkeypatch: pytest.MonkeyPatch) -> None:
    """Backoff and rate-limit sleeps make the retry tests slow for no gain."""
    monkeypatch.setattr(mod.time, "sleep", lambda _seconds: None)


def _patch_post(monkeypatch: pytest.MonkeyPatch, responses: list[Any]) -> list[dict[str, Any]]:
    """Serves `responses` in order; returns the list that records each call."""
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], headers: dict[str, str], timeout: int):
        calls.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        outcome = responses[min(len(calls) - 1, len(responses) - 1)]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(mod.requests, "post", fake_post)
    return calls


def test_normalize_emits_fields_in_schema_order():
    """The pandas UDF's frame is matched to the struct positionally, so order is a contract."""
    record = mod._normalize_filing_period(_node())

    assert record is not None
    assert list(record.keys()) == [field.name for field in mod.filing_period_schema.fields]


def test_normalize_coerces_id_and_dates():
    record = mod._normalize_filing_period(_node())

    assert record is not None
    assert record["databaseId"] == 12345
    assert isinstance(record["databaseId"], int)
    assert record["startOn"] == pd.Timestamp("2026-01-02")
    assert record["endOn"] == pd.Timestamp("2026-03-06")
    assert record["createdAt"] == pd.Timestamp("2025-01-02T03:04:05Z")
    assert record["updatedAt"] == pd.Timestamp("2025-06-07T08:09:10Z")


def test_normalized_record_survives_conversion_to_the_declared_schema():
    """`start_on`/`end_on` are DateType while the record carries pd.Timestamp.

    Pandas UDF results cross Arrow on the way back to Spark, so the declared struct is
    what the normalized frame has to satisfy. Asserting Python-level equality alone would
    not catch a field whose pandas dtype cannot reach its Spark type, so this converts a
    real frame against the real schema.
    """
    pa = pytest.importorskip("pyarrow")

    spark_to_arrow = {
        "TimestampType()": pa.timestamp("us"),
        "IntegerType()": pa.int32(),
        "StringType()": pa.string(),
        "DateType()": pa.date32(),
    }
    frame = pd.DataFrame(
        [mod._normalize_filing_period(_node()), mod._normalize_filing_period(_node(databaseId=2))]
    )
    schema = pa.schema(
        [
            pa.field(field.name, spark_to_arrow[repr(field.dataType)])
            for field in mod.filing_period_schema.fields
        ]
    )

    table = pa.Table.from_pandas(frame, schema=schema, preserve_index=False)

    assert table.column("startOn").to_pylist()[0] == date(2026, 1, 2)
    assert table.column("endOn").to_pylist()[0] == date(2026, 3, 6)
    assert table.column("databaseId").to_pylist() == [12345, 2]


def test_normalize_keeps_a_period_with_no_dates():
    """An open-ended filing period is still worth storing; only its dates are unknown."""
    record = mod._normalize_filing_period(_node(startOn=None, endOn=None))

    assert record is not None
    assert record["startOn"] is pd.NaT
    assert record["endOn"] is pd.NaT


@pytest.mark.parametrize(
    "node",
    [
        pytest.param({}, id="node_of_another_type"),
        pytest.param(_node(databaseId=None), id="no_database_id"),
        pytest.param(_node(id=None), id="no_encoded_id"),
        pytest.param(_node(databaseId="not-a-number"), id="unparsable_database_id"),
    ],
)
def test_normalize_rejects_unusable_nodes(node: dict[str, Any]):
    assert mod._normalize_filing_period(node) is None


def test_batch_drops_unresolvable_nodes_and_keeps_the_rest(monkeypatch: pytest.MonkeyPatch):
    """One unresolvable id used to abort its whole batch; the survivors are the point."""
    payload = {"data": {"nodes": [_node(databaseId=1), None, {}, _node(databaseId=4)]}}
    _patch_post(monkeypatch, [_FakeResponse(payload)])

    nodes = mod._get_filing_periods_batch([1, 2, 3, 4], "token")

    assert [node["databaseId"] for node in nodes] == [1, 4]


def test_batch_requests_every_id_encoded(monkeypatch: pytest.MonkeyPatch):
    calls = _patch_post(monkeypatch, [_FakeResponse({"data": {"nodes": []}})])

    mod._get_filing_periods_batch([7, 8], "token")

    assert calls[0]["json"]["variables"]["ids"] == [
        mod._base64_encode_id(7),
        mod._base64_encode_id(8),
    ]
    assert calls[0]["headers"]["Authorization"] == "Bearer token"


def test_batch_retries_a_retryable_status_then_succeeds(monkeypatch: pytest.MonkeyPatch):
    calls = _patch_post(
        monkeypatch,
        [
            _FakeResponse({}, status_code=429),
            _FakeResponse({"data": {"nodes": [_node(databaseId=9)]}}),
        ],
    )

    nodes = mod._get_filing_periods_batch([9], "token")

    assert len(calls) == 2
    assert [node["databaseId"] for node in nodes] == [9]


def test_batch_retries_a_connection_error_then_succeeds(monkeypatch: pytest.MonkeyPatch):
    calls = _patch_post(
        monkeypatch,
        [
            requests.exceptions.ConnectionError("reset by peer"),
            _FakeResponse({"data": {"nodes": [_node(databaseId=9)]}}),
        ],
    )

    nodes = mod._get_filing_periods_batch([9], "token")

    assert len(calls) == 2
    assert [node["databaseId"] for node in nodes] == [9]


def test_batch_raises_after_exhausting_attempts(monkeypatch: pytest.MonkeyPatch):
    calls = _patch_post(monkeypatch, [requests.exceptions.ConnectionError("reset by peer")])

    with pytest.raises(RuntimeError):
        mod._get_filing_periods_batch([9], "token", max_attempts=3)

    assert len(calls) == 3


@pytest.mark.parametrize("status_code", [401, 403, 404])
def test_batch_stops_retrying_a_non_retryable_status(monkeypatch: pytest.MonkeyPatch, status_code: int):
    """A credential or missing-resource problem will not improve by asking again."""
    calls = _patch_post(monkeypatch, [_FakeResponse({}, status_code=status_code)])

    with pytest.raises(RuntimeError):
        mod._get_filing_periods_batch([9], "token")

    assert len(calls) == 1


@pytest.mark.parametrize("status_code", sorted(mod._RETRYABLE_STATUS_CODES))
def test_batch_exhausts_attempts_on_a_persistent_retryable_status(
    monkeypatch: pytest.MonkeyPatch, status_code: int
):
    """The counterpart to the test above: these statuses are worth all three attempts."""
    calls = _patch_post(monkeypatch, [_FakeResponse({}, status_code=status_code)])

    with pytest.raises(RuntimeError):
        mod._get_filing_periods_batch([9], "token", max_attempts=3)

    assert len(calls) == 3


def test_batch_returns_empty_when_graphql_reports_errors(monkeypatch: pytest.MonkeyPatch):
    """A GraphQL-level error is not an HTTP error; the ids stay missing and get retried."""
    _patch_post(
        monkeypatch,
        [_FakeResponse({"data": None, "errors": [{"message": "something went wrong"}]})],
    )

    assert mod._get_filing_periods_batch([9], "token") == []


def test_backoff_grows_and_is_capped():
    delays = [mod._backoff_seconds(attempt, cap=8.0) for attempt in range(1, 8)]

    assert delays[0] < delays[1] < delays[2]
    assert all(delay <= 8.0 + 0.25 for delay in delays)
