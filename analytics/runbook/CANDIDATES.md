# Calibration candidates ledger

Last consolidation pass: **none yet** (treat as past the nudge threshold once the ledger has entries)

Shared and committed, unlike the personal, gitignored `CALIBRATION_*.md` logs. Decouples observing
something from proposing a doc edit: a run appends a line here instead of proposing an edit when an
observation is below the promotion bar, a data-state finding awaits confirmation, or a Track 2
process-design candidate is parked. Rows are never deleted; only the `status` column and the
last-consolidation date above are ever updated.

When to append, the promotion thresholds, and the branch/PR mechanics are owned by the calibration
step of the win-analytics-process skill
(`.claude/skills/win-analytics-process/references/calibration.md`) — this file does not restate them.

- **Entry format** (one table row per observation): date | track (`data`/`process`) | tag
  (`universal`/`data-state`) | one-line observation | run reference (branch or ticket) | status.

## Ledger

| date | track | tag | observation | run ref | status |
|---|---|---|---|---|---|
