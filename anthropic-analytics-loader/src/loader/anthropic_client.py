"""Client for the Claude Enterprise Analytics API (organizations/analytics/*).

Requires an Analytics API key (`read:analytics` scope), created by the org's primary owner at
claude.ai/admin-settings/api-access. Not interchangeable with an Admin API key.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import requests
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

ANALYTICS_BASE = "https://api.anthropic.com/v1/organizations/analytics"
ANTHROPIC_VERSION = "2023-06-01"
USER_AGENT = "goodparty-anthropic-analytics-loader/1.0"
MAX_WINDOW_DAYS = 31  # API limit per request for bucket_width=1d

log = structlog.get_logger()


class RetryableHTTPError(RuntimeError):
    """A 429/5xx response worth retrying."""


def _headers(api_key: str) -> dict[str, str]:
    return {"x-api-key": api_key, "anthropic-version": ANTHROPIC_VERSION, "User-Agent": USER_AGENT}


@retry(
    retry=retry_if_exception_type(RetryableHTTPError),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _get(path: str, api_key: str, params: dict[str, Any]) -> dict[str, Any]:
    resp = requests.get(f"{ANALYTICS_BASE}/{path}", headers=_headers(api_key), params=params, timeout=30)
    if resp.status_code == 429 or resp.status_code >= 500:
        raise RetryableHTTPError(f"{path}: {resp.status_code} {resp.text[:200]}")
    resp.raise_for_status()
    return resp.json()


def _paginate_buckets(path: str, api_key: str, base_params: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield each `data[]` time-bucket across all pages."""
    page = None
    while True:
        params = dict(base_params)
        if page:
            params["page"] = page
        body = _get(path, api_key, params)
        yield from body["data"]
        if not body.get("has_more"):
            return
        page = body["next_page"]


def _paginate_rows(path: str, api_key: str, base_params: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Yield each top-level `data[]` row across all pages (non-bucketed endpoints)."""
    page = None
    while True:
        params = dict(base_params)
        if page:
            params["page"] = page
        body = _get(path, api_key, params)
        yield from body["data"]
        if not body.get("has_more"):
            return
        page = body["next_page"]


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


_CENTS = Decimal(100)


def cents_to_dollars(raw: str | None) -> Decimal | None:
    """cost_report/user_cost_report `amount`/`list_amount` are decimal strings in fractional
    cents (e.g. "41280.000000" == $412.80) -- see
    https://platform.claude.com/docs/en/manage-claude/analytics-api#working-with-the-api.
    Parsed as Decimal, never float, since values can run into the millions of dollars.
    """
    if raw is None:
        return None
    return Decimal(raw) / _CENTS


def _date_chunks(start: datetime, end: datetime, max_days: int = MAX_WINDOW_DAYS):
    cur = start
    while cur < end:
        chunk_end = min(cur + timedelta(days=max_days), end)
        yield cur, chunk_end
        cur = chunk_end


def fetch_org_usage_report(api_key: str, start: datetime, end: datetime, now: datetime) -> list[dict]:
    """ORG_USAGE_REPORT: daily org-wide token usage, broken out by product and model."""
    rows = []
    for chunk_start, chunk_end in _date_chunks(start, end):
        params = {
            "starting_at": chunk_start.isoformat(),
            "ending_at": chunk_end.isoformat(),
            "bucket_width": "1d",
            "limit": MAX_WINDOW_DAYS,
            "group_by[]": ["product", "model"],
        }
        for bucket in _paginate_buckets("usage_report", api_key, params):
            b_start = parse_ts(bucket["starting_at"])
            b_end = parse_ts(bucket["ending_at"])
            for r in bucket["results"]:
                cache_creation = r.get("cache_creation") or {}
                server_tool_use = r.get("server_tool_use") or {}
                rows.append(
                    {
                        "bucket_start": b_start,
                        "bucket_end": b_end,
                        "product": r.get("product"),
                        "model": r.get("model"),
                        "uncached_input_tokens": r.get("uncached_input_tokens", 0),
                        "cache_read_input_tokens": r.get("cache_read_input_tokens", 0),
                        "cache_creation_1h_input_tokens": cache_creation.get("ephemeral_1h_input_tokens", 0),
                        "cache_creation_5m_input_tokens": cache_creation.get("ephemeral_5m_input_tokens", 0),
                        "output_tokens": r.get("output_tokens", 0),
                        "requests": r.get("requests"),
                        "web_search_requests": server_tool_use.get("web_search_requests", 0),
                        "ingested_at": now,
                        "raw_json": json.dumps(r),
                    }
                )
    return rows


def fetch_org_cost_report(api_key: str, start: datetime, end: datetime, now: datetime) -> list[dict]:
    """ORG_COST_REPORT: daily org-wide cost in USD, broken out by product, model, cost type, and
    token type. `token_type` is only non-null where `cost_type == "tokens"` -- code_execution and
    web_search costs have no token-type breakdown. `requests` is always null here because
    `group_by` includes `cost_type`/`token_type` (the API nulls `requests` whenever either is
    grouped); it isn't dropped by this loader.
    """
    rows = []
    for chunk_start, chunk_end in _date_chunks(start, end):
        params = {
            "starting_at": chunk_start.isoformat(),
            "ending_at": chunk_end.isoformat(),
            "bucket_width": "1d",
            "limit": MAX_WINDOW_DAYS,
            "group_by[]": ["product", "model", "cost_type", "token_type"],
        }
        for bucket in _paginate_buckets("cost_report", api_key, params):
            b_start = parse_ts(bucket["starting_at"])
            b_end = parse_ts(bucket["ending_at"])
            for r in bucket["results"]:
                rows.append(
                    {
                        "bucket_start": b_start,
                        "bucket_end": b_end,
                        "product": r.get("product"),
                        "model": r.get("model"),
                        "cost_type": r.get("cost_type"),
                        "token_type": r.get("token_type"),
                        "amount": cents_to_dollars(r.get("amount")),
                        "list_amount": cents_to_dollars(r.get("list_amount")),
                        "currency": r.get("currency"),
                        "requests": r.get("requests"),
                        "ingested_at": now,
                        "raw_json": json.dumps(r),
                    }
                )
    return rows


def fetch_user_cost_report(api_key: str, start: datetime, end: datetime, now: datetime) -> list[dict]:
    """USER_COST_REPORT: per-user cost in USD, ranked by spend, one row per user per 31-day window.

    The API caps a single request's range at 31 days, so a longer backfill becomes multiple rows
    per user (one per window) rather than one aggregate over the whole requested range.
    """
    rows = []
    for chunk_start, chunk_end in _date_chunks(start, end):
        params = {
            "starting_at": chunk_start.isoformat(),
            "ending_at": chunk_end.isoformat(),
            "limit": 1000,
            "order_by": "amount",
            "order": "desc",
        }
        for r in _paginate_rows("user_cost_report", api_key, params):
            actor = r.get("actor") or {}
            rows.append(
                {
                    "period_start": chunk_start,
                    "period_end": chunk_end,
                    "user_id": actor.get("user_id"),
                    "user_email": actor.get("email"),
                    "user_name": actor.get("name"),
                    "user_deleted": actor.get("deleted"),
                    "amount": cents_to_dollars(r.get("amount")),
                    "list_amount": cents_to_dollars(r.get("list_amount")),
                    "currency": r.get("currency"),
                    "requests": r.get("requests"),
                    "ingested_at": now,
                    "raw_json": json.dumps(r),
                }
            )
    return rows


def fetch_summaries(api_key: str, start: datetime, end: datetime, now: datetime) -> list[dict]:
    """SUMMARIES: daily org-wide active-user / adoption snapshot."""
    rows = []
    params = {"starting_date": start.date().isoformat(), "ending_date": end.date().isoformat()}
    body = _get("summaries", api_key, params)
    for r in body.get("summaries", []):
        row = dict(r)
        row["starting_at"] = parse_ts(r["starting_at"])
        row["ending_at"] = parse_ts(r["ending_at"])
        row["ingested_at"] = now
        row["raw_json"] = json.dumps(r)
        rows.append(row)
    return rows


def fetch_users_daily(api_key: str, start: date, end: date, now: datetime) -> list[dict]:
    """USERS_DAILY: one row per user per day, flattened to top-level adoption/spend metrics.

    Looped one day at a time (rather than passing starting_date/ending_date in one call) since
    the per-row response doesn't carry back which day a range-query row belongs to.
    """
    rows = []
    day = start
    while day < end:
        params = {"date": day.isoformat(), "limit": 1000}
        page = None
        while True:
            p = dict(params)
            if page:
                p["page"] = page
            body = _get("users", api_key, p)
            for r in body.get("data", []):
                user = r.get("user") or {}
                chat = r.get("chat_metrics") or {}
                code = (r.get("claude_code_metrics") or {}).get("core_metrics") or {}
                cowork = r.get("cowork_metrics") or {}
                rows.append(
                    {
                        "activity_date": day,
                        "user_id": user.get("id"),
                        "user_email": user.get("email_address"),
                        "chat_message_count": chat.get("message_count", 0),
                        "chat_distinct_skills_used_count": chat.get("distinct_skills_used_count"),
                        "chat_distinct_connectors_used_count": chat.get("distinct_connectors_used_count"),
                        "claude_code_commit_count": code.get("commit_count", 0),
                        "claude_code_pull_request_count": code.get("pull_request_count", 0),
                        "claude_code_lines_added": (code.get("lines_of_code") or {}).get("added_count", 0),
                        "claude_code_lines_removed": (code.get("lines_of_code") or {}).get(
                            "removed_count", 0
                        ),
                        "cowork_message_count": cowork.get("message_count", 0),
                        "cowork_skills_used_count": cowork.get("skills_used_count", 0),
                        "cowork_connectors_used_count": cowork.get("connectors_used_count", 0),
                        "web_search_count": r.get("web_search_count", 0),
                        "ingested_at": now,
                        "raw_json": json.dumps(r),
                    }
                )
            if not body.get("next_page"):
                break
            page = body["next_page"]
        day += timedelta(days=1)
    return rows


def utcnow() -> datetime:
    return datetime.now(UTC)
