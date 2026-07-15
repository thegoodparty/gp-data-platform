# Calibration candidates ledger

Last consolidation pass: **2026-06-15** (engagement.md dedup)

Shared and committed, unlike the personal, gitignored `CALIBRATION_*.md` logs. Decouples observing
something from proposing a doc edit: a run appends a line here instead of proposing an edit when an
observation is below the promotion bar, a data-state finding awaits confirmation, or a Track 2
process-design candidate is parked. Rows are never deleted; only the `status` column and the
last-consolidation date above are ever updated.

When to append, the promotion thresholds, and the branch/PR mechanics are owned by the calibration
step of the analytics-process skill
(`.claude/skills/analytics-process/references/calibration.md`) — this file does not restate them.

- **Entry format** (one table row per observation): date | track (`data`/`process`) | tag
  (`universal`/`data-state`) | one-line observation | run reference (branch or ticket) | status.

## Ledger

| date | track | tag | observation | run ref | status |
|---|---|---|---|---|---|
| 2026-06-11 | process | universal | analytics env lacks declared Databricks deps: `analytics/pyproject.toml` declares only pandas, but `databricks_conn.run_query` needs `databricks-sql-connector` + `databricks-sdk`, so every DB run requires `uv run --with ...`. Candidate: add a runtime dependency group. | diagnostics baseline arms 2026-06-11 (reps 1-2 + express-framing) | parked (Track 2 OFF) |
| 2026-06-11 | process | data-state | `analytics/diagnostics/baseline_question.md` base numbers drifted: reproduced denominators +12-16/month (~0.15%) above the answer key, consistent with live-table drift or a day-boundary convention. Candidate: refresh the recorded base or pin the boundary explicitly. | diagnostics baseline arms 2026-06-11 (reps 1-2 + express-framing) | promoted (calib/process/2026-07-15-brief-boundary-semantics; 2nd occurrence DATA-2114 was a directed verification, which substitutes for the 3rd data-state sighting per calibration.md — decomposition query confirmed both mechanisms: boundary reading 6-34/mo + live-mart drift ~0.1-0.3%) |
| 2026-07-15 | process | universal | `brief-schema.md` `target.definition` has no per-metric source field (only the population gets `source_model`); the R4 cold executor had to resolve `first_campaign_sent_at`'s home via the knowledge skill. It did so losslessly, so the layering worked as designed — promote only if metric-home ambiguity recurs as real friction. | DATA-2114 R4 arm (`analytics/diagnostics/logs/r4_brief_handoff_20260715.md`) | parked (Track 2 OFF) |
