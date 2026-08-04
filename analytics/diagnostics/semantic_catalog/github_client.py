"""Minimal GitHub REST client for the governance-thread workflow.

Stdlib-only, injectable urlopen seam (matches slack_reply/clickup_client).
Token travels in the Authorization header, never on the command line. Note two
tokens exist at the call sites: the workflow GITHUB_TOKEN (PR files/reviews/
comments) and ORG_READ_TOKEN (org team membership); this class is token-agnostic
and gets instantiated once per token.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from typing import Any

API = "https://api.github.com"
_PER_PAGE = 100


class GitHubClient:
    def __init__(self, token: str, repo: str, *, urlopen: Callable[..., Any] = urllib.request.urlopen):
        self._token = token
        self.repo = repo
        self._urlopen = urlopen

    def _request(self, method: str, path: str, payload: dict | None = None) -> Any:
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(f"{API}{path}", data=data, method=method)
        req.add_header("Authorization", f"Bearer {self._token}")
        req.add_header("Accept", "application/vnd.github+json")
        if data is not None:
            req.add_header("Content-type", "application/json; charset=utf-8")
        with self._urlopen(req, timeout=30) as resp:
            return json.load(resp)

    def _paginate(self, path: str) -> list[Any]:
        out: list[Any] = []
        page = 1
        while True:
            sep = "&" if "?" in path else "?"
            batch = self._request("GET", f"{path}{sep}per_page={_PER_PAGE}&page={page}")
            out.extend(batch)
            if len(batch) < _PER_PAGE:
                return out
            page += 1

    def pr_files(self, pr_number: int) -> list[str]:
        return [f["filename"] for f in self._paginate(f"/repos/{self.repo}/pulls/{pr_number}/files")]

    def pr_reviews(self, pr_number: int) -> list[dict]:
        return self._paginate(f"/repos/{self.repo}/pulls/{pr_number}/reviews")

    def team_members(self, org: str, team_slug: str) -> list[str]:
        return [m["login"] for m in self._paginate(f"/orgs/{org}/teams/{team_slug}/members")]

    def issue_comments(self, pr_number: int) -> list[dict]:
        return self._paginate(f"/repos/{self.repo}/issues/{pr_number}/comments")

    def create_comment(self, pr_number: int, body: str) -> int:
        return int(
            self._request("POST", f"/repos/{self.repo}/issues/{pr_number}/comments", {"body": body})["id"]
        )

    def update_comment(self, comment_id: int, body: str) -> None:
        self._request("PATCH", f"/repos/{self.repo}/issues/comments/{comment_id}", {"body": body})
