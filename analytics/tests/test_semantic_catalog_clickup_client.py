import io
import urllib.error
import urllib.request

import pytest
from semantic_catalog.clickup_client import ClickUpClient
from semantic_catalog.sigma_tasks import TaskPayload


def _client_with_recorder(responses):
    """A ClickUpClient whose HTTP seam is replaced by a canned-response recorder."""
    calls = []
    client = ClickUpClient(token="tok")

    def fake(method, path, body=None):
        calls.append((method, path, body))
        return responses.pop(0)

    client._http_json = fake  # type: ignore[method-assign]
    return client, calls


def test_find_returns_task_id_when_present():
    client, calls = _client_with_recorder(
        [{"tasks": [{"id": "abc", "custom_fields": [{"id": "field1", "value": "m@2026-07-28"}]}]}]
    )
    got = client.find_task_by_build_key("901", "field1", "m@2026-07-28")
    assert got == "abc"
    method, path, _ = calls[0]
    assert method == "GET"
    assert "/list/901/task" in path
    assert "field1" in path and "m%40" in path  # custom_fields filter urlencoded


def test_find_rejects_substring_collision():
    """ClickUp's "=" filter on short text can be a substring match, so a returned
    task whose field value is a different, superset key must not be treated as a hit.
    """
    client, _ = _client_with_recorder(
        [{"tasks": [{"id": "abc", "custom_fields": [{"id": "field1", "value": "win_users@2026-07-28"}]}]}]
    )
    got = client.find_task_by_build_key("901", "field1", "users@2026-07-28")
    assert got is None


def test_find_returns_none_when_absent():
    client, _ = _client_with_recorder([{"tasks": []}])
    assert client.find_task_by_build_key("901", "field1", "m@2026-07-28") is None


def test_http_json_wraps_httperror(monkeypatch):
    def fake_urlopen(req, timeout=30):
        raise urllib.error.HTTPError(
            "https://api.clickup.com/api/v2/list/901/task",
            400,
            "Bad Request",
            hdrs=None,
            fp=io.BytesIO(b'{"err":"bad field"}'),
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = ClickUpClient(token="tok")
    payload = TaskPayload(name="n", markdown_description="d", build_key="k@2026-07-28")
    with pytest.raises(RuntimeError, match="400"):
        client.create_task("901", payload, "field1")


def test_http_json_wraps_urlerror(monkeypatch):
    def fake_urlopen(req, timeout=30):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    client = ClickUpClient(token="tok")
    payload = TaskPayload(name="n", markdown_description="d", build_key="k@2026-07-28")
    with pytest.raises(RuntimeError, match="connection refused"):
        client.create_task("901", payload, "field1")


def test_create_posts_name_description_and_build_key_field():
    client, calls = _client_with_recorder([{"id": "new1"}])
    payload = TaskPayload(name="Build in Sigma: M (m)", markdown_description="body", build_key="m@2026-07-28")
    got = client.create_task("901", payload, "field1")
    assert got == "new1"
    method, path, body = calls[0]
    assert method == "POST" and path == "/list/901/task"
    assert body["name"] == "Build in Sigma: M (m)"
    assert body["markdown_description"] == "body"
    assert {"id": "field1", "value": "m@2026-07-28"} in body["custom_fields"]
