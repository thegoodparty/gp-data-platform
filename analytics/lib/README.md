# analytics/lib

Committed, reusable building blocks for Win-product analyses, so ad-hoc notebooks
stop rebuilding the same cohort and engagement logic from scratch each run.

## Why this exists

The Win event-family allowlist (a large `CASE WHEN event_type LIKE ...` block) and
the consolidated per-user working set were being retyped in every analysis. That is
a token cost, a time cost, and a transcription-drift risk. This package holds one
copy. See the runbook (`analytics/runbook/win.md`) section 4 for the taxonomy this
mirrors and section 7 for the build-once-slice-many pattern.

## What's here

`win_analysis.py`:
- `win_event_predicate(include_partial=False)` — the drift-controlled Win event
  allowlist as a SQL predicate string. Pattern-based, so new event_types in known
  families classify automatically.
- `build_win_working_set(run_query, cohorts, ...)` — builds one per-user
  `cohort x engagement` DataFrame with all funnel steps anchored point-in-time to
  the election date, carrying the standard section-6 slicing dimensions so re-cuts
  (e.g. ICP vs not) need no new query.
- `wilson(k, n)` — Wilson 95% score interval.

## Usage from a notebook

This module is connection-agnostic: you inject a `run_query(sql) -> DataFrame`
callable. With the gp-ai-projects shared client:

```python
import sys; sys.path.insert(0, "analytics/lib")   # adjust to repo-relative path
from shared.databricks_client import DatabricksClient   # via uv run --project ../gp-ai-projects
import win_analysis as wa

run_query = DatabricksClient().execute_query
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
- The 2025-08-01 drift cutoff is encoded by the explicit exclusions in
  `_CORE_PREDICATE` (drops `Dashboard - Campaign Plan Viewed` and `win_briefings`),
  not parameterized by date.

## Follow-up: dbt event-taxonomy model (planned)

The durable single source of truth is a planned dbt model `int__amplitude_event_taxonomy`
(pattern-based: `event_type, family, is_win, is_recurrent, first_seen_date`) that both
dbt models and notebooks read, replacing the hardcoded event lists in
`int__amplitude_win_activity` / `int__amplitude_user_milestones` and this predicate.
Until that lands, keep `_CORE_PREDICATE` in sync with runbook section 4. Ticket: DATA-1945.
