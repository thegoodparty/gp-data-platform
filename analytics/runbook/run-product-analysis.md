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
5. **Results checkpoint. HARD GATE.** Present the executed results and the robustness checks to the user and STOP. Do not spawn any reviewer until the user gives explicit approval to proceed. The user may redirect scope here, which is cheaper than a wasted review cycle. A clean-looking result or an eager requester is not a reason to skip this gate.
6. **Review.** Spawn the `product-data-scientist` (methodology + interpretation) and `product-manager` (usefulness) reviewers as sub-agents on the executed analysis.
7. **Capture calibration (two tracks).** Run the closing calibration pass (`.claude/skills/win-analytics-process/references/calibration.md`).
   - **Track 1 — data calibration (default, on).** Reusable data facts, joins, gotchas, metric definitions, coverage, canonical-list additions. Route to the owning `win-analytics-knowledge` doc. Propose the exact edit; apply only after explicit human approval.
   - **Track 2 — process calibration (default, OFF).** Changes to the framing routine, the executor / `analytics/lib`, the reviewer agents, or the process skill itself. On a standard run, do NOT propose or apply these — park them in the log under a "Process-design candidates" heading, state in the closing summary that process calibration was OFF (not run), and offer to rerun in process-design mode. Act on Track 2 only if the user has explicitly opted into process-design mode this session.
   - No core or shared file is edited without explicit human approval. Approved edits follow the calibration branch/PR convention in `.claude/skills/win-analytics-process/references/calibration.md`.

Seed steps 2–7 as a TodoWrite list at the start so the staged flow (including the results checkpoint and the two calibration tracks) is not skipped.

## Troubleshooting

- "Missing required Databricks connection parameters" → 1Password is locked; unlock it and open a fresh shell.
- The orchestrator jumps straight to an answer → it skipped framing; restart from step 2 and seed the to-do.
- A concept resolves to several candidate metrics → resolve it through `.claude/skills/win-analytics-knowledge/references/canonical_metrics.md`, which is the governed registry.
