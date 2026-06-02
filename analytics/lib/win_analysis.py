"""Reusable helpers for Win-product engagement analyses.

Single Python source of truth for the Win event-family allowlist (mirrors
``analytics/runbook/win.md`` section 4) and the consolidated per-user working
set. Keeping the family logic here means ad-hoc notebooks stop retyping a large
``CASE WHEN event_type LIKE ...`` block per query, which is both a token cost and
a transcription-drift risk.

This module is connection-agnostic: callers inject a ``run_query(sql) -> DataFrame``
callable, so it works equally with the gp-ai-projects ``DatabricksClient`` or an
inline ``databricks.sql.connect`` cursor.

Keep this in sync with runbook section 4 until the dbt ``int__amplitude_event_taxonomy``
model supersedes both (see README.md). The drift cutoff (2025-08-01) is currently
encoded by the explicit exclusions below, not parameterized by date; a true
date-parameterized cutoff awaits the taxonomy model's ``first_seen_date`` column.
"""

from __future__ import annotations

import math
import re
from typing import Callable, Mapping

import pandas as pd

EVENTS_TABLE = (
    "goodparty_data_catalog.dbt_staging.stg_airbyte_source__amplitude_api_events"
)
USERS_WIN_CANDIDACY = "goodparty_data_catalog.mart_analytics.users_win_candidacy"

# Drift-controlled core Win event families (runbook section 4). Pattern-based so
# new event_types within these families classify automatically. Excludes families
# first-seen after the 2025-08-01 drift cutoff ('Dashboard - Campaign Plan Viewed',
# first-seen 2026-04-09; win_briefings, first-seen 2026-04-10) and all anonymous /
# cross-product noise (autotrack, session, navigation, auth, experiment, serve).
_CORE_PREDICATE = """(
   event_type LIKE 'Onboarding -%' OR event_type LIKE 'Onboarding:%' OR event_type IN ('onboarding_complete','Invalid Party','Sign Up Clicked')
   OR (event_type LIKE 'Dashboard -%' AND event_type <> 'Dashboard - Campaign Plan Viewed')
   OR event_type LIKE 'Voter Outreach -%' OR event_type LIKE 'Outreach -%'
   OR event_type LIKE 'Schedule Text Campaign%' OR event_type LIKE 'schedule_campaign%'
   OR event_type LIKE 'Content Builder%' OR event_type LIKE 'ai_content_%' OR event_type LIKE 'campaign_assistant%'
   OR event_type LIKE 'Voter Data -%' OR event_type LIKE 'Voter Data:%' OR event_type LIKE 'Download Voter%' OR event_type LIKE 'Custom Voter%'
   OR event_type LIKE 'Profile -%'
   OR event_type LIKE 'Pro Upgrade -%' OR event_type LIKE 'Pro Upgrade:%' OR event_type = 'pro_upgrade_complete'
   OR event_type LIKE 'P2P Upgrade -%' OR event_type LIKE 'Candidate Website%'
   OR event_type LIKE 'Campaign Verify%' OR event_type LIKE 'Campaign Plan%' OR event_type LIKE '10 DLC Compliance%'
   OR event_type LIKE 'AI Assistant%' OR event_type = 'question_complete' OR event_type LIKE 'Resources -%'
)"""

# Partial-ramp families: real Win activity, but first-seen mid-September 2025
# (Candidacy 2025-09-18, Contacts 2025-09-19), so only partially present in a
# pre-Nov-2025 window. Reported alongside core so sensitivity to them is visible.
_PARTIAL_PREDICATE = "(event_type LIKE 'Candidacy -%' OR event_type LIKE 'Contacts -%')"

# Slicing dimensions available on users_win_candidacy with no join (runbook section 6).
# Carried on the working set so re-cuts (e.g. ICP vs not) need no new query.
DEFAULT_DIMS = (
    "icp_office_win",
    "office_type",
    "election_level",
    "campaign_party",
    "campaign_state",
)


def win_event_predicate(include_partial: bool = False) -> str:
    """Return the SQL boolean predicate selecting drift-controlled Win events.

    Pass ``include_partial=True`` to also include the partial-ramp families
    (Candidacy, Contacts). The canonical headline metric uses core only.
    """
    if include_partial:
        return f"({_CORE_PREDICATE} OR {_PARTIAL_PREDICATE})"
    return _CORE_PREDICATE


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
) -> pd.DataFrame:
    """Build one consolidated per-user cohort x engagement working set, slice it in pandas.

    This is the "build once, slice many" pattern (runbook section 7). All funnel
    steps are derived point-in-time from the raw event stream (events strictly
    before each user's election anchor), so onboarding/activation are anchored
    rather than as-of-today.

    Args:
        run_query: callable taking a SQL string and returning a DataFrame.
        cohorts: mapping of cohort_label -> {"filter": <WHERE predicate on
            users_win_candidacy>, "anchor": <SQL agg expr for the per-user anchor
            date, e.g. "MIN(CAST(general_election_date AS DATE))">}.
        slice_dims: users_win_candidacy columns to carry for free re-cuts.
        preelection_days: width of the pre-election window (84 = 12 weeks).
        event_floor: lower bound on event_time to bound the scan (real Win
            coverage starts ~2025-04-24; default 2025-01-01 excludes junk).

    Returns:
        One row per (user_id, cohort) with columns: the slice_dims, plus
        any_core, onboarded, dash_viewed, activated (0/1 funnel flags, anchored),
        core_all, core_distinct_types, corepartial_all, core_preelection (counts),
        and beyond_signup (1 if the user touched >= 2 *distinct* core event types,
        i.e. engaged past account creation; runbook section 3.5).

    Security:
        ``cohorts[*]["filter"]`` and ``["anchor"]`` are interpolated verbatim as SQL
        expressions (they are WHERE/SELECT fragments and cannot be parameterized), so
        they must come from trusted, analyst-controlled code only -- never from
        external or user-supplied input. ``event_floor`` is format-validated below.
    """
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", event_floor):
        raise ValueError(f"event_floor must be YYYY-MM-DD, got: {event_floor!r}")
    core = win_event_predicate(include_partial=False)
    partial = _PARTIAL_PREDICATE
    dim_cols = "".join(f", MAX({d}) AS {d}" for d in slice_dims)

    cohort_selects = []
    for label, spec in cohorts.items():
        cohort_selects.append(
            f"""  SELECT user_id, '{label}' AS cohort, {spec['anchor']} AS anchor{dim_cols}
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
    MAX(CASE WHEN {core} THEN 1 ELSE 0 END) AS any_core,
    MAX(CASE WHEN event_type = 'onboarding_complete' THEN 1 ELSE 0 END) AS onboarded,
    MAX(CASE WHEN event_type = 'Dashboard - Candidate Dashboard Viewed' THEN 1 ELSE 0 END) AS dash_viewed,
    MAX(CASE WHEN event_type = 'Voter Outreach - Campaign Completed' THEN 1 ELSE 0 END) AS activated,
    SUM(CASE WHEN {core} THEN 1 ELSE 0 END) AS core_all,
    COUNT(DISTINCT CASE WHEN {core} THEN event_type END) AS core_distinct_types,
    SUM(CASE WHEN {core} OR {partial} THEN 1 ELSE 0 END) AS corepartial_all,
    SUM(CASE WHEN ({core}) AND e.event_time >= co.anchor - INTERVAL {preelection_days} DAYS THEN 1 ELSE 0 END) AS core_preelection
  FROM cohort co
  JOIN {EVENTS_TABLE} e
    ON try_cast(e.user_id AS bigint) = co.user_id
   AND e.event_time < co.anchor
   AND e.event_time >= DATE'{event_floor}'
  GROUP BY co.user_id, co.cohort
)
SELECT co.*,
  COALESCE(ev.any_core, 0) AS any_core,
  COALESCE(ev.onboarded, 0) AS onboarded,
  COALESCE(ev.dash_viewed, 0) AS dash_viewed,
  COALESCE(ev.activated, 0) AS activated,
  COALESCE(ev.core_all, 0) AS core_all,
  COALESCE(ev.core_distinct_types, 0) AS core_distinct_types,
  COALESCE(ev.corepartial_all, 0) AS corepartial_all,
  COALESCE(ev.core_preelection, 0) AS core_preelection
FROM cohort co
LEFT JOIN ev ON ev.user_id = co.user_id AND ev.cohort = co.cohort
"""
    df = run_query(sql)
    # "Engaged beyond account creation" is >= 2 *distinct* core event types
    # (runbook section 3.5), not >= 2 event rows: a single feature double-fired
    # must not count. core_all is kept for intensity analyses.
    df["beyond_signup"] = (df["core_distinct_types"] >= 2).astype(int)
    return df
