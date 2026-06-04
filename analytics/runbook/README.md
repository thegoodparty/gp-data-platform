# analytics/runbook

The Win product-analytics runbook now lives as **two skills**, not a single `win.md` monolith:

- **`.claude/skills/win-analytics-knowledge/`** — the data facts (sources, joins, outcomes,
  engagement, viability, segmentation, gotchas) behind a thin router (`SKILL.md`) that resolves
  a concept to its one governed definition in `references/canonical_metrics.md`.
- **`.claude/skills/win-analytics-process/`** — the analyst workflow: scoping methodology,
  reusable analysis patterns, the brief contract, the pipeline topology, and the calibration-log
  convention.

Start at either skill's `SKILL.md`. A Serve runbook will live alongside these when it exists.

## What still lives here

- **`CALIBRATION_<YYYY-MM-DD>.md`** — per-session calibration logs. These are gitignored working
  documents; durable lessons are promoted into the two skills above (and the agent files). See
  `.claude/skills/win-analytics-process/references/calibration.md` for the convention.
