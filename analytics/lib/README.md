# analytics/lib

Committed, reusable building blocks for product analyses (Win and Serve), so ad-hoc
notebooks stop rebuilding the same cohort and engagement logic from scratch each run.

## Why this exists

The Win event-family allowlist and the consolidated per-user working set were being
retyped in every analysis. That is a token cost, a time cost, and a transcription-drift
risk. This package holds one copy. The event classification is now sourced from the dbt
model `int__amplitude_event_catalog` (DATA-1945) so there is a single source of truth
shared by the dbt models and notebooks. See the win-analytics-knowledge skill's
`references/engagement.md` for the family taxonomy and the analytics-process skill's
`references/methodology.md` for the build-once-slice-many pattern.

## What's here

`win_analysis.py`:
- `win_event_predicate(drift_cutoff="2026-01-01")` — the drift-controlled Win event
  allowlist as a SQL predicate string. Reads `int__amplitude_event_catalog`
  (`is_win` for membership; `first_seen_date <= drift_cutoff` for drift control), so
  it stays in sync with the dbt models automatically. Returns an
  `event_type IN (<subquery>)` expression — use it in a WHERE/HAVING/JOIN, not inside
  a scalar CASE (Spark restricts IN-subqueries to filter contexts).
- `build_win_working_set(run_query, cohorts, ..., drift_cutoff="2026-01-01")` — builds
  one per-user `cohort x engagement` DataFrame with all funnel steps anchored
  point-in-time to the election date, carrying the standard slicing dimensions from the
  win-analytics-knowledge skill's `references/segmentation.md` so re-cuts (e.g. ICP vs not) need no new query. Win events are tagged via
  a LEFT JOIN to the taxonomy.
- `wilson(k, n)` — Wilson 95% score interval.

`serve_analysis.py` (DATA-2116):
- `serve_engagement_predicate()` — the broad-Serve-engagement surface test as a SQL
  predicate (2025 `family = 'serve'` events + the 2026 event generation by prefix +
  `Viewed` on Serve-surface paths). Surface test only: population, post-activation
  time-scope, in-session, and hygiene conditions are the caller's job — prefer the
  builder below, which applies all of them.
- `build_serve_working_set(run_query, cohorts, ...)` — one per-user
  `cohort x engagement` DataFrame of broad Serve engagement: in-session
  (`session_id != -1`, excludes server-emitted dispatches), scoped to
  `event_time >= eo_activated_at` (default anchor), impersonation-tainted sessions
  and `@goodparty.org` excluded. Definition and caveats: the
  serve-analytics-knowledge skill's `references/methodology_defaults.md`.
- `wilson` is re-exported from `win_analysis` (one implementation).

## Usage from a notebook

This module is connection-agnostic: you inject a `run_query(sql) -> DataFrame`
callable. The repo-standard connection is the profile-auth helper
`databricks_conn.py`, which authenticates via the `~/.databrickscfg` profile
(`databricks auth login`) with no PAT and no cross-repo dependency:

```python
import sys; sys.path.insert(0, "analytics/lib")   # adjust to repo-relative path
import databricks_conn as dbc
import win_analysis as wa

run_query = dbc.run_query
cohorts = {
    "nov2025_general": {
        "filter": "general_election_date BETWEEN '2025-11-01' AND '2025-11-30'",
        "anchor": "MIN(CAST(general_election_date AS DATE))",
    },
    "may2026_primary": {
        "filter": "primary_election_date BETWEEN '2026-05-01' AND '2026-05-31'",
        "anchor": "MIN(CAST(primary_election_date AS DATE))",
    },
}
df = wa.build_win_working_set(run_query, cohorts)
# Then slice in pandas. Funnel: df.groupby("cohort")[["any_core","beyond_signup","onboarded"]].mean()
# Free re-cut: df.groupby(["cohort","icp_office_win"])["beyond_signup"].mean()
```

## Caveats

- Slice dimensions are collapsed to one value per user via `MAX()`. Fine for the
  common one-latest-candidacy user; a multi-cycle user gets the max. `groupby` on a
  dimension drops NULLs by default (e.g. the ~45% NULL `election_level` bucket), so
  pass `dropna=False` when you want NULL as a category.
- Drift control is the `drift_cutoff` date (default `2026-01-01`), applied as
  `first_seen_date <= drift_cutoff` against the taxonomy. The default keeps all 2025
  product families and excludes the 2026 drift families (`win_briefings`,
  `Dashboard - Campaign Plan Viewed`). Pass a tighter `drift_cutoff` (e.g. relative to
  your cohort window) for a stricter coverage-comparability check. The former
  core/partial split is retired — a sensitivity check is just a second run with a
  different cutoff.
- Reads `goodparty_data_catalog.dbt.int__amplitude_event_catalog` (prod). That table
  must exist (a prod dbt run of the model); it does as of DATA-1945.
- `build_win_working_set` is **election-anchored with a backward window** (it joins
  events strictly *before* each user's anchor date). It does NOT fit signup-anchored,
  forward-window analyses (onboarding completion, time-to-activation); for those reuse
  only `win_event_predicate` + `wilson` and build the cohort logic notebook-local.
- The `onboarded` funnel flag keys on `event_type = 'onboarding_complete'`, which is
  FALSE for users in the new onboarding flow (post 2026-05-07). For cross-cutover
  onboarding analyses use `Onboarding - Candidate Pledge Completed` instead (win-analytics-knowledge skill's `references/engagement.md`).

## Single source of truth: the dbt event-taxonomy model

`int__amplitude_event_catalog` (`event_type, family, is_win, is_recurrent,
first_seen_date`; DATA-1945) is the one classifier. The dbt models
(`int__amplitude_win_activity` / `_weekly`) consume it via `is_recurrent`, and this
helper consumes it via `is_win` + `first_seen_date`. There is no longer a separate
hardcoded allowlist to keep in sync.
