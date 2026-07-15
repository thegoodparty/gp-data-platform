"""Reusable helpers for Serve-product engagement analyses.

Implements the **broad Serve engagement** definition (serve-analytics-knowledge
skill's references/methodology_defaults.md, decided DATA-2115 / built DATA-2116):
an event counts when it is in-session (``session_id != -1``), fired at or after
the user's ``eo_activated_at``, on a Serve surface (2025 ``family = 'serve'``
events, the 2026 event generation by prefix, or a ``Viewed`` on a Serve-surface
path), under the standard hygiene filter (non-internal email, impersonation-
tainted sessions excluded).

Two Serve-specific mechanics differ from ``win_analysis``:

- The 2026 Serve event generation (Briefing Assistant, Community Issues, Org
  Switcher) is not classified by ``int__amplitude_event_catalog`` (falls to
  family ``other``), so classification here is family + prefix + path rather
  than taxonomy-only.
- Several new events are server-emitted dispatches (100% ``session_id = -1``),
  and staff impersonation sessions carry the EO's ``user_id`` with no property
  marker — both are excluded structurally, not by event name.

This module is connection-agnostic: callers inject a ``run_query(sql) -> DataFrame``
callable. The repo-standard one is ``databricks_conn.run_query`` (profile auth via
``~/.databrickscfg``); any ``run_query(sql) -> DataFrame`` callable works.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping

import pandas as pd
from win_analysis import wilson  # shared; one implementation

__all__ = [
    "EVENTS_TABLE",
    "USERS_SERVE_BASE",
    "EVENT_TAXONOMY",
    "SERVE_EVENT_PREFIXES",
    "SERVE_SURFACE_PATH_PREFIXES",
    "serve_engagement_predicate",
    "build_serve_working_set",
    "wilson",
]

# Fully-qualified prod table paths, hardcoded for the same reason as
# win_analysis.py: this module runs outside dbt (notebooks / ad-hoc), where
# ``ref()`` can resolve to stale dev artifacts. Repoint for a dev/test catalog.
EVENTS_TABLE = "goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events"
USERS_SERVE_BASE = "goodparty_data_catalog.mart_analytics.users_serve_base"
EVENT_TAXONOMY = "goodparty_data_catalog.dbt.int__amplitude_event_catalog"

# The 2026 Serve event generation, instrumented 2026-05/06 (dated via the omni
# provenance CSV). Unclassified by the taxonomy macro as of 2026-07 — extend
# this tuple when omni ships a new Serve event family the taxonomy still lacks.
SERVE_EVENT_PREFIXES = (
    "Briefing Assistant -",
    "Community Issues -",
    "Org Switcher -",
)

# Serve product surfaces for path-anchored `Viewed` engagement. Shared-surface
# interaction events under Win names are deliberately NOT counted (attribution
# safety over recall; methodology_defaults.md).
SERVE_SURFACE_PATH_PREFIXES = (
    "/dashboard/polls",
    "/dashboard/briefings",
    "/dashboard/contacts",
    "/dashboard/chief-of-staff",
    "/dashboard/outreach",
    "/dashboard/community",
    "/serve",
    "/polls",
)

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_ANCHOR_FORBIDDEN = re.compile(r"[;'\"]|--|/\*")
# Filters legitimately contain quoted literals (eo_activated_at >= DATE'...'),
# so their tripwire only bans statement separators and comment tokens.
_FILTER_FORBIDDEN = re.compile(r";|--|/\*")


def _require_date(label: str, value: str) -> None:
    """Validate a YYYY-MM-DD date string before it is interpolated into SQL."""
    if not _DATE_RE.fullmatch(value):
        raise ValueError(f"{label} must be YYYY-MM-DD, got: {value!r}")


def _sql_quote(s: str) -> str:
    """Escape a trusted constant for a single-quoted SQL literal."""
    return s.replace("'", "''")


def _prefix_like_any(col: str, prefixes: tuple[str, ...]) -> str:
    """OR-chain of LIKE 'prefix%' predicates over the given column."""
    return " OR ".join(f"{col} LIKE '{_sql_quote(p)}%'" for p in prefixes)


def serve_engagement_predicate(events_alias: str = "e", path_expr: str | None = None) -> str:
    """Return the SQL predicate for a broad-Serve-engagement event.

    Covers the surface test only (family / prefix / path): the caller owns the
    population, time-scope (``event_time >= eo_activated_at``), in-session, and
    hygiene conditions — ``build_serve_working_set`` applies all of them; use it
    unless you are building a custom query. The family test is an IN-subquery,
    so use the predicate in a WHERE / HAVING / JOIN clause, not inside a scalar
    CASE expression.

    Args:
        events_alias: alias of the events table in the calling query.
        path_expr: SQL expression yielding the event's page path. Defaults to
            extracting it from ``event_properties`` on the aliased raw events
            table; pass a plain column (e.g. ``"e.path"``) when the calling
            query has already extracted the path.
    """
    a = events_alias
    path = path_expr or f"{a}.event_properties:path::string"
    return (
        f"({a}.event_type IN (SELECT event_type FROM {EVENT_TAXONOMY} WHERE family = 'serve')\n"
        f"   OR {_prefix_like_any(f'{a}.event_type', SERVE_EVENT_PREFIXES)}\n"
        f"   OR ({a}.event_type = 'Viewed' AND ({_prefix_like_any(path, SERVE_SURFACE_PATH_PREFIXES)})))"
    )


# Slicing dimensions available on users_serve_base with no join
# (serve-analytics-knowledge skill's references/segmentation.md). Office/level/
# state are pending officeholder ingestion and intentionally absent.
DEFAULT_DIMS = (
    "serve_onboarding_steps_completed",
    "is_active_serve_user",
    "has_pledged",
    "registration_year",
)


def build_serve_working_set(
    run_query: Callable[[str], pd.DataFrame],
    cohorts: Mapping[str, Mapping[str, str]],
    *,
    slice_dims: tuple[str, ...] = DEFAULT_DIMS,
    event_floor: str = "2025-01-01",
) -> pd.DataFrame:
    """Build one consolidated per-user cohort x engagement working set, slice it in pandas.

    The "build once, slice many" pattern (analytics-process skill's
    references/methodology.md), for Serve. Engagement is broad Serve engagement:
    in-session, post-activation, surface-scoped, hygiene-filtered, impersonation-
    tainted sessions excluded (serve-analytics-knowledge skill's
    references/methodology_defaults.md owns the definition).

    Args:
        run_query: callable taking a SQL string and returning a DataFrame.
        cohorts: mapping of cohort_label -> {"filter": <WHERE predicate on
            users_serve_base rows, ANDed with is_serve_user>, "anchor": <SQL
            expr for the per-user engagement-scope start; default
            "eo_activated_at">}. The anchor is the Serve analog of Win's
            election anchor, but scopes engagement to *at or after* it.
        slice_dims: users_serve_base columns to carry for free re-cuts.
        event_floor: lower bound on event_time to bound the scan.

    Returns:
        One row per (user_id, cohort) with columns: the slice_dims, plus
        any_engaged (0/1), engaged_all (event count), engaged_distinct_types,
        engaged_days (distinct active dates), last_engaged_at, and
        beyond_first_touch (1 if >= 2 distinct engaged event types).

    Security:
        ``cohorts[*]["filter"]`` and ``["anchor"]`` are interpolated verbatim as
        SQL expressions, so they must come from trusted, analyst-controlled code
        only -- never from external or user-supplied input. ``event_floor`` is
        date-validated, ``slice_dims`` are validated as plain SQL identifiers,
        ``anchor`` passes a tripwire rejecting quotes, ``;``, and comment
        tokens (expressions like ``COALESCE(...)`` stay legal), and ``filter``
        passes a looser tripwire rejecting ``;`` and comment tokens (quoted
        literals stay legal -- a filter cannot be parameterized).
    """
    _require_date("event_floor", event_floor)
    for dim in slice_dims:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dim):
            raise ValueError(f"slice_dims element must be a plain SQL identifier, got: {dim!r}")
    dim_cols = "".join(f", {d}" for d in slice_dims)

    cohort_selects = []
    for label, spec in cohorts.items():
        anchor = spec.get("anchor", "eo_activated_at")
        # Tripwire, not a grammar: anchors are trusted expressions (see
        # Security note) and may be COALESCE(...)/CAST(...), but they never
        # legitimately need quotes, statement separators, or comment tokens.
        if _ANCHOR_FORBIDDEN.search(anchor):
            raise ValueError(f"anchor must not contain quotes, ';', or comment tokens: {anchor!r}")
        if _FILTER_FORBIDDEN.search(spec["filter"]):
            raise ValueError(f"filter must not contain ';' or comment tokens: {spec['filter']!r}")
        cohort_selects.append(
            f"""  SELECT user_id, '{_sql_quote(label)}' AS cohort, {anchor} AS anchor{dim_cols}
  FROM {USERS_SERVE_BASE}
  WHERE is_serve_user AND ({spec["filter"]})"""
        )
    cohort_sql = "\n  UNION ALL\n".join(cohort_selects)

    sql = f"""
WITH cohort AS (
{cohort_sql}
),
ev AS (
  SELECT try_cast(e.user_id AS bigint) AS user_id, e.session_id, e.event_type,
    e.event_time, e.event_properties:path::string AS path
  FROM {EVENTS_TABLE} e
  WHERE e.event_time >= DATE'{event_floor}'
    AND try_cast(e.user_id AS bigint) IS NOT NULL
    AND (e.user_properties:email::string NOT ILIKE '%@goodparty.org'
         OR e.user_properties:email::string IS NULL)
),
-- Impersonation taint: any session that touched /impersonate or /admin under
-- this user_id is staff-driven (gotchas.md; no property marker exists). Scanned
-- from the raw table with a 30-day lookback before event_floor so a session
-- that straddles the floor still carries its taint (session_id is the
-- session-start epoch; real sessions span hours, not weeks).
tainted AS (
  SELECT DISTINCT try_cast(user_id AS bigint) AS user_id, session_id
  FROM {EVENTS_TABLE}
  WHERE (event_properties:path::string = '/impersonate'
         OR event_properties:path::string LIKE '/admin%')
    AND event_time >= DATE'{event_floor}' - INTERVAL 30 DAYS
    AND try_cast(user_id AS bigint) IS NOT NULL
),
engaged AS (
  SELECT co.user_id, co.cohort,
    COUNT(*) AS engaged_all,
    COUNT(DISTINCT e.event_type) AS engaged_distinct_types,
    COUNT(DISTINCT DATE(e.event_time)) AS engaged_days,
    MAX(e.event_time) AS last_engaged_at
  FROM cohort co
  JOIN ev e
    ON e.user_id = co.user_id
   AND e.event_time >= co.anchor
   AND e.session_id != -1
  LEFT JOIN tainted t
    ON t.user_id = e.user_id AND t.session_id = e.session_id
  WHERE t.session_id IS NULL
    AND {serve_engagement_predicate("e", path_expr="e.path")}
  GROUP BY co.user_id, co.cohort
)
SELECT co.*,
  CASE WHEN eng.user_id IS NOT NULL THEN 1 ELSE 0 END AS any_engaged,
  COALESCE(eng.engaged_all, 0) AS engaged_all,
  COALESCE(eng.engaged_distinct_types, 0) AS engaged_distinct_types,
  COALESCE(eng.engaged_days, 0) AS engaged_days,
  eng.last_engaged_at
FROM cohort co
LEFT JOIN engaged eng ON eng.user_id = co.user_id AND eng.cohort = co.cohort
"""
    df = run_query(sql)
    # Mirrors Win's beyond_signup: >= 2 *distinct* engaged event types, so a
    # single surface double-firing does not count as sustained engagement.
    df["beyond_first_touch"] = (df["engaged_distinct_types"] >= 2).astype(int)
    return df
