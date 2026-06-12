# Calibration candidates ledger

Shared, committed, append-only. Decouples observing something from proposing a doc edit: a run
appends a line here instead of proposing an edit when an observation is below the promotion bar,
a data-state finding awaits confirmation, or a Track 2 process-design candidate is parked. When
to append and how promotion works is defined in the calibration step of the win-analytics-process
skill (`.claude/skills/win-analytics-process/references/calibration.md`).

- **Entry format** (one table row per observation): date | track (`data`/`process`) | tag
  (`universal`/`data-state`) | one-line observation | run reference (branch or ticket) | status.
- **Promotion:** essentially the same observation appearing in **2 separate runs** (**3** for
  `data-state`-tagged items) becomes eligible for the next calibration batch as a real proposal,
  subject to the promotion test and the human approval gate. When promoted, set the status of the
  matching lines to `promoted: <PR>` — never delete lines.
- **Repo mechanics:** appends follow the standard branch/PR convention, no exemptions. If a run
  produces a calibration batch PR, appends ride in that PR; a run with only candidate
  observations gets a small ledger-only PR on a `calib/data` branch with slug suffix
  `-candidates`. Append-only PRs should be quick to approve, but approval is still required and
  merging stays a human action.

## Ledger

| date | track | tag | observation | run ref | status |
|---|---|---|---|---|---|
