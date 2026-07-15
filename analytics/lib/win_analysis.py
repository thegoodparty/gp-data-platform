"""Reusable helpers for Win-product engagement analyses.

Win event classification is sourced from the single-source dbt model
``int__amplitude_event_catalog`` (DATA-1945) rather than a hand-maintained
``CASE WHEN event_type LIKE ...`` block: an event is "Win" when its taxonomy
``is_win`` flag is true, and drift is controlled by
``first_seen_date <= drift_cutoff`` (features first seen after the cutoff are
excluded so coverage is comparable across a cohort). The former core/partial
split is retired in favor of that single date cutoff -- run with a different
cutoff for a sensitivity check.

This module is connection-agnostic: callers inject a ``run_query(sql) -> DataFrame``
callable. The repo-standard one is ``databricks_conn.run_query`` (profile auth via
``~/.databrickscfg``); any ``run_query(sql) -> DataFrame`` callable works.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping

import pandas as pd

# Fully-qualified prod table paths. These are intentionally hardcoded to the
# ``goodparty_data_catalog`` prod schemas (``dbt`` for staging + intermediates,
# ``mart_analytics`` for marts; the legacy ``dbt_staging`` schema is being retired)
# rather than resolved via dbt
# ``ref()``: this module runs outside dbt (notebooks / ad-hoc), where ``ref()`` can
# resolve to stale dev artifacts (analytics-process skill's references/methodology.md).
# Repoint these for a dev/test catalog.
EVENTS_TABLE = "goodparty_data_catalog.dbt.stg_airbyte_source__amplitude_api_events"
USERS_WIN_CANDIDACY = "goodparty_data_catalog.mart_analytics.users_win_candidacy"
EVENT_TAXONOMY = "goodparty_data_catalog.dbt.int__amplitude_event_catalog"

# Default drift cutoff: keep all 2025 product families in scope and exclude the
# 2026 drift families (win_briefings, 'Dashboard - Campaign Plan Viewed', etc.).
# Tighten per analysis (e.g. relative to a cohort's window) for stricter coverage
# comparability.
DEFAULT_DRIFT_CUTOFF = "2026-01-01"

_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_ANCHOR_FORBIDDEN = re.compile(r"[;'\"]|--|/\*")

# Slicing dimensions available on users_win_candidacy with no join
# (win-analytics-knowledge skill's references/segmentation.md).
# Carried on the working set so re-cuts (e.g. ICP vs not) need no new query.
DEFAULT_DIMS = (
    "icp_office_win",
    "office_type",
    "election_level",
    "campaign_party",
    "campaign_state",
)


def _require_date(label: str, value: str) -> None:
    """Validate a YYYY-MM-DD date string before it is interpolated into SQL."""
    if not _DATE_RE.fullmatch(value):
        raise ValueError(f"{label} must be YYYY-MM-DD, got: {value!r}")


def _sql_quote(s: str) -> str:
    """Escape a trusted constant for a single-quoted SQL literal."""
    return s.replace("'", "''")


def _win_event_types_sql(drift_cutoff: str) -> str:
    """Subquery selecting drift-controlled Win event_types from the taxonomy."""
    _require_date("drift_cutoff", drift_cutoff)
    return (
        f"SELECT event_type FROM {EVENT_TAXONOMY} "
        f"WHERE is_win AND first_seen_date <= DATE'{drift_cutoff}'"
    )


def win_event_predicate(drift_cutoff: str = DEFAULT_DRIFT_CUTOFF) -> str:
    """Return a SQL predicate selecting drift-controlled Win-product events.

    Win membership comes from ``int__amplitude_event_catalog.is_win``; drift is
    controlled by ``first_seen_date <= drift_cutoff``. Supersedes the former
    hand-maintained LIKE allowlist.

    The result is an ``event_type IN (<subquery>)`` expression. Spark restricts
    IN-subqueries to filter contexts, so use it in a WHERE / HAVING / JOIN clause,
    not inside a scalar CASE expression. (``build_win_working_set`` uses the same
    taxonomy filter via a LEFT JOIN for exactly that reason.)

    Args:
        drift_cutoff: YYYY-MM-DD. Only event_types first seen on or before this
            date count as Win. Default keeps 2025 families and drops the 2026 ones.
    """
    return f"event_type IN ({_win_event_types_sql(drift_cutoff)})"


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    """Return (point_pct, lo_pct, hi_pct) Wilson score interval for k successes of n."""
    if n == 0:
        return (float("nan"), float("nan"), float("nan"))
    if not 0 <= k <= n:
        # Guards against the opaque "math domain error" that k > n would otherwise
        # raise inside math.sqrt (p*(1-p) goes negative). Catches swapped args, e.g.
        # wilson(n, k).
        raise ValueError(f"wilson() requires 0 <= k <= n, got k={k}, n={n}")
    p = k / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    margin = (z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)) / den
    return (
        round(100 * p, 1),
        round(100 * (center - margin), 1),
        round(100 * (center + margin), 1),
    )


def build_win_working_set(
    run_query: Callable[[str], pd.DataFrame],
    cohorts: Mapping[str, Mapping[str, str]],
    *,
    slice_dims: tuple[str, ...] = DEFAULT_DIMS,
    preelection_days: int = 84,
    event_floor: str = "2025-01-01",
    drift_cutoff: str = DEFAULT_DRIFT_CUTOFF,
) -> pd.DataFrame:
    """Build one consolidated per-user cohort x engagement working set, slice it in pandas.

    This is the "build once, slice many" pattern (analytics-process skill's
    references/methodology.md). All funnel
    steps are derived point-in-time from the raw event stream (events strictly
    before each user's election anchor), so onboarding/activation are anchored
    rather than as-of-today. Win events are identified by a LEFT JOIN to the
    drift-controlled Win event_types from ``int__amplitude_event_catalog``.

    Args:
        run_query: callable taking a SQL string and returning a DataFrame.
        cohorts: mapping of cohort_label -> {"filter": <WHERE predicate on
            users_win_candidacy>, "anchor": <SQL agg expr for the per-user anchor
            date, e.g. "MIN(CAST(general_election_date AS DATE))">}.
        slice_dims: users_win_candidacy columns to carry for free re-cuts.
        preelection_days: width of the pre-election window (84 = 12 weeks).
        event_floor: lower bound on event_time to bound the scan (real Win
            coverage starts ~2025-04-24; default 2025-01-01 excludes junk).
        drift_cutoff: YYYY-MM-DD; only event_types first seen on or before this
            date count as Win (see win_event_predicate). Run with a tighter cutoff
            for a coverage-sensitivity check.

    Returns:
        One row per (user_id, cohort) with columns: the slice_dims, plus
        any_core, onboarded, dash_viewed, activated (0/1 funnel flags, anchored),
        core_all, core_distinct_types, core_preelection (counts), and beyond_signup
        (1 if the user touched >= 2 *distinct* Win event types, i.e. engaged past
        account creation; win-analytics-knowledge skill's references/engagement.md).

    Security:
        ``cohorts[*]["filter"]`` and ``["anchor"]`` are interpolated verbatim as SQL
        expressions (they are WHERE/SELECT fragments and cannot be parameterized), so
        they must come from trusted, analyst-controlled code only -- never from
        external or user-supplied input. ``event_floor`` and ``drift_cutoff`` are
        date-validated, ``slice_dims`` are validated as plain SQL identifiers,
        ``preelection_days`` must be a positive integer, cohort labels are
        escaped for the SQL literal, and ``anchor`` passes a tripwire rejecting
        quotes, ``;``, and comment tokens (expressions like ``MIN(CAST(...))``
        stay legal).
    """
    _require_date("event_floor", event_floor)
    if preelection_days <= 0:
        raise ValueError(f"preelection_days must be a positive integer, got: {preelection_days!r}")
    for dim in slice_dims:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", dim):
            raise ValueError(f"slice_dims element must be a plain SQL identifier, got: {dim!r}")
    win_types = _win_event_types_sql(drift_cutoff)  # also validates drift_cutoff
    dim_cols = "".join(f", MAX({d}) AS {d}" for d in slice_dims)

    cohort_selects = []
    for label, spec in cohorts.items():
        # Tripwire, not a grammar (mirrors serve_analysis): anchors are trusted
        # expressions (MIN(CAST(...)) is legal) but never legitimately need
        # quotes, statement separators, or comment tokens.
        if _ANCHOR_FORBIDDEN.search(spec["anchor"]):
            raise ValueError(f"anchor must not contain quotes, ';', or comment tokens: {spec['anchor']!r}")
        cohort_selects.append(
            f"""  SELECT user_id, '{_sql_quote(label)}' AS cohort, {spec['anchor']} AS anchor{dim_cols}
  FROM {USERS_WIN_CANDIDACY}
  WHERE is_latest_version AND NOT is_demo AND ({spec['filter']})
  GROUP BY user_id"""
        )
    cohort_sql = "\n  UNION ALL\n".join(cohort_selects)

    sql = f"""
WITH cohort AS (
{cohort_sql}
),
ev AS (
  SELECT co.user_id, co.cohort,
    MAX(CASE WHEN wt.event_type IS NOT NULL THEN 1 ELSE 0 END) AS any_core,
    MAX(CASE WHEN e.event_type = 'onboarding_complete' THEN 1 ELSE 0 END) AS onboarded,
    MAX(CASE WHEN e.event_type = 'Dashboard - Candidate Dashboard Viewed' THEN 1 ELSE 0 END) AS dash_viewed,
    MAX(CASE WHEN e.event_type = 'Voter Outreach - Campaign Completed' THEN 1 ELSE 0 END) AS activated,
    SUM(CASE WHEN wt.event_type IS NOT NULL THEN 1 ELSE 0 END) AS core_all,
    COUNT(DISTINCT CASE WHEN wt.event_type IS NOT NULL THEN e.event_type END) AS core_distinct_types,
    SUM(CASE WHEN wt.event_type IS NOT NULL AND e.event_time >= co.anchor - INTERVAL {preelection_days} DAYS THEN 1 ELSE 0 END) AS core_preelection
  FROM cohort co
  JOIN {EVENTS_TABLE} e
    ON try_cast(e.user_id AS bigint) = co.user_id
   AND e.event_time < co.anchor
   AND e.event_time >= DATE'{event_floor}'
  LEFT JOIN ({win_types}) wt
    ON wt.event_type = e.event_type
  GROUP BY co.user_id, co.cohort
)
SELECT co.*,
  COALESCE(ev.any_core, 0) AS any_core,
  COALESCE(ev.onboarded, 0) AS onboarded,
  COALESCE(ev.dash_viewed, 0) AS dash_viewed,
  COALESCE(ev.activated, 0) AS activated,
  COALESCE(ev.core_all, 0) AS core_all,
  COALESCE(ev.core_distinct_types, 0) AS core_distinct_types,
  COALESCE(ev.core_preelection, 0) AS core_preelection
FROM cohort co
LEFT JOIN ev ON ev.user_id = co.user_id AND ev.cohort = co.cohort
"""
    df = run_query(sql)
    # "Engaged beyond account creation" is >= 2 *distinct* Win event types
    # (win-analytics-knowledge skill's references/engagement.md), not >= 2 event rows: a single feature double-fired
    # must not count. core_all is kept for intensity analyses.
    df["beyond_signup"] = (df["core_distinct_types"] >= 2).astype(int)
    return df
