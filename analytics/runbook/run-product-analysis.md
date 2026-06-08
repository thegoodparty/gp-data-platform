Start a product-analytics session on the right path: refine the question to an approved brief before writing any analysis code, then execute, then review.

## Prerequisites

- **Databricks access.** `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_API_KEY` must be set (the key resolves from 1Password; unlock it if a call errors with "Missing required Databricks connection parameters"). The live `goodparty_data_catalog` is ground truth.
- **Skills to load.** Read the `win-analytics-process` skill (how to work) and resolve every data concept through the `win-analytics-knowledge` skill (what is true). Do not restate the pipeline flow; it lives in `.claude/skills/win-analytics-process/references/pipeline.md`.
- **Working-set helper.** `analytics/lib/win_analysis.py` builds the per-user working set once; slice it in pandas.

## Steps

1. **Determine the product.** Today only the **Win** product is supported; route to the Win skills above. (Serve and others will be added here later.)
2. **Frame (refine the question).** Run the framing routine in `.claude/skills/win-analytics-process/references/framing.md` in this conversation: ask the user clarifying questions, resolve scoping forks, and propose a framing (population, eligibility, target, comparison, cohorts). Do not write the brief or any analysis code yet.
3. **Get human approval of the framing.** This is the hard gate between framing and execution. Once the user approves, produce the structured brief per `.claude/skills/win-analytics-process/references/brief-schema.md` as the handoff artifact. Do not proceed to execution until the user approves.
4. **Execute.** Build the working set once with `analytics/lib/win_analysis.py`, then slice every cut in pandas. Save the brief alongside the executed notebook (ad-hoc: `analytics/ad_hoc/<YYYY-MM-DD>_<brief_id>_brief.yaml`).
5. **Review.** Spawn the `product-data-scientist` (methodology + interpretation) and `product-manager` (usefulness) reviewers as sub-agents on the executed analysis.
6. **Capture calibration.** Run the closing calibration pass (`.claude/skills/win-analytics-process/references/calibration.md`): route findings into the file that owns them, or record that none were needed.

Seed steps 2–6 as a TodoWrite list at the start so the staged flow is not skipped.

## Troubleshooting

- "Missing required Databricks connection parameters" → 1Password is locked; unlock it and open a fresh shell.
- The orchestrator jumps straight to an answer → it skipped framing; restart from step 2 and seed the to-do.
- A concept resolves to several candidate metrics → resolve it through `.claude/skills/win-analytics-knowledge/references/canonical_metrics.md`, which is the governed registry.
