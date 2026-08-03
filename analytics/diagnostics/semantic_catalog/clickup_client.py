"""Thin ClickUp REST v2 client for interim Sigma-build tasks (DATA-2199).

Stdlib only. All HTTP goes through _http_json so tests can replace one seam and
never touch the network. The API token is passed in by the caller (read from the
CLICKUP_TASK_TOKEN env var in cli.py); nothing here reads the environment.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from semantic_catalog.sigma_tasks import TaskPayload


class ClickUpClient:
    def __init__(self, token: str, api_base: str = "https://api.clickup.com/api/v2") -> None:
        self._token = token
        self._api_base = api_base.rstrip("/")

    def _http_json(self, method: str, path: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(f"{self._api_base}{path}", data=data, method=method)
        req.add_header("Authorization", self._token)
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:  # fixed api base, not user input
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:500]
            raise RuntimeError(f"ClickUp API {method} {path} failed: HTTP {e.code} {detail}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"ClickUp API {method} {path} failed: {e.reason}") from e

    def find_task_by_build_key(self, list_id: str, field_id: str, build_key: str) -> str | None:
        cf = json.dumps([{"field_id": field_id, "operator": "=", "value": build_key}])
        query = urllib.parse.urlencode({"custom_fields": cf, "include_closed": "true"})
        resp = self._http_json("GET", f"/list/{list_id}/task?{query}")
        # ClickUp's "=" filter can be a substring match on short text, so confirm an
        # EXACT build-key match client-side before treating it as a dedupe hit.
        for task in resp.get("tasks") or []:
            for field in task.get("custom_fields") or []:
                if field.get("id") == field_id and field.get("value") == build_key:
                    return task["id"]
        return None

    def create_task(self, list_id: str, payload: TaskPayload, field_id: str) -> str:
        body = {
            "name": payload.name,
            "markdown_description": payload.markdown_description,
            "custom_fields": [{"id": field_id, "value": payload.build_key}],
        }
        resp = self._http_json("POST", f"/list/{list_id}/task", body)
        return resp["id"]
