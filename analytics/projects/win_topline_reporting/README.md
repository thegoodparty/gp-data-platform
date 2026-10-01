# win_topline_reporting

Recurring Win top-line reports. Each script prints to stdout and stamps its as-of time; the marts
refresh hourly, so quote figures with the timestamp.

- `topline_report.py`: accounts, activated and Pro by election bucket (brief: `win_topline_reporting_brief.yaml`).
- `election_cohort.py`: census of users with an election on one date (default 2026-11-03), Pro, reached
  voters, split by the onboarding ballot answer, with a product-DB send reference and a roster-corroboration
  read. Brief: `election_cohort_brief.yaml`; every number's standalone SQL: `election_cohort_queries.md`.
  Re-run weekly through election day (DATA-2599, DATA-2603).

Run from `analytics/`: `uv run python projects/win_topline_reporting/<script>.py`.
