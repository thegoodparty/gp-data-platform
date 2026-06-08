# analytics/runbook

Routing index for product-analytics work. Match your task to a row, then open that artifact. Start at the entry-point runbook.

| Type | Trigger keywords | Path | Description |
|---|---|---|---|
| proc | start analysis, product analysis, new analysis, frame question, kick off, where do I start | `analytics/runbook/run-product-analysis.md` | Entry point. Primes the session and seeds the staged to-do: frame → approve → execute → review → calibrate. |
| ref | how to work, scoping, methodology, brief, pipeline, calibration, reviewers | `.claude/skills/win-analytics-process/` (`SKILL.md`) | The analyst workflow: framing routine, methodology, brief contract, pipeline topology, calibration. |
| ref | data facts, which table, which metric, joins, engagement, outcomes, viability, segmentation, gotchas | `.claude/skills/win-analytics-knowledge/` (`SKILL.md`) | The data facts behind Win analyses, behind a router over the canonical metrics registry. |

A Serve entry will be added here when it exists.

## What still lives here

- **`CALIBRATION_<YYYY-MM-DD>.md`** — per-session calibration logs. Gitignored working documents; durable lessons are promoted into the two skills above (and the agent files). See `.claude/skills/win-analytics-process/references/calibration.md` for the convention.
