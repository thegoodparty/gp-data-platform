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
| 2026-06-11 | process | data-state | `analytics/diagnostics/baseline_question.md` base numbers drifted: reproduced denominators +12-16/month (~0.15%) above the answer key, consistent with live-table drift or a day-boundary convention. Candidate: refresh the recorded base or pin the boundary explicitly. | diagnostics baseline arms 2026-06-11 (reps 1-2 + express-framing) | parked (Track 2 OFF) |
| 2026-07-15 | process | universal | Express path (DATA-2120 mechanism 4) scoped but deferred to its own PR. Settled design: folded single gate (express plan + one confirm); one-line close (or read-back rows if anything appended); non-negotiables kept = knowledge-skill resolution, live-catalog verification, boundary-semantics statement. Hardening required before shipping: falsifiable eligibility (any judgment call during concept resolution = fork = full pipeline), ineligible-request override recorded in the close line, "express — unreviewed" tag on the answer itself (not just the close), a to-do-seeding clause in `run-product-analysis.md` step 2, and a calibration.md carve-out so skipping the pass isn't a silent violation. Full design + risk notes in DATA-2120 comment (2026-07-15). | DATA-2120 scoping session 2026-07-15 | parked (deferred to own express PR) |
