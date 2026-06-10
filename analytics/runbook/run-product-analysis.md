Start a product-analytics session on the right path: refine the question to an approved brief before writing any analysis code, then execute, then review.

## Prerequisites

- **Databricks access.** Analytics connects via `analytics/lib/databricks_conn.run_query`, which authenticates with the Databricks SDK profile in `~/.databrickscfg` (set up once with `databricks auth login`; honors `DATABRICKS_CONFIG_PROFILE`). `DATABRICKS_HTTP_PATH` (the SQL warehouse path) must be set; no PAT is used. The live `goodparty_data_catalog` is ground truth.
- **Skills to load.** Read the `win-analytics-process` skill (how to work) and resolve every data concept through the `win-analytics-knowledge` skill (what is true). Do not restate the pipeline flow; it lives in `.claude/skills/win-analytics-process/references/pipeline.md`.
- **Working-set helper.** `analytics/lib/win_analysis.py` builds the per-user working set once; slice it in pandas.

## Steps

1. **Determine the product, and ask the profiling opt-in.** Today only the **Win** product is supported; route to the Win skills above. (Serve and others will be added here later.) At the start, also ask the user once whether to **profile this run's pipeline cost** — default **no**. This is opt-in instrumentation (see Step 8); most runs skip it. Record the answer.
2. **Frame (refine the question).** Run the framing routine in `.claude/skills/win-analytics-process/references/framing.md` in this conversation: ask the user clarifying questions, resolve scoping forks, and propose a framing (population, eligibility, target, comparison, cohorts). Do not write the brief or any analysis code yet.
3. **Get human approval of the framing.** This is the hard gate between framing and execution. Before producing the brief, settle the deliverable format (notebook, Python script, or both) and the save location with the user, per `framing.md` step 5. Once the user approves, produce the structured brief per `.claude/skills/win-analytics-process/references/brief-schema.md` (recording the agreed `execution_notes.preferred_format` and save location) as the handoff artifact. Do not proceed to execution until the user approves.
4. **Execute.** Build the working set once with `analytics/lib/win_analysis.py`, then slice every cut in pandas. Produce the deliverable in the brief's `execution_notes.preferred_format` (notebook, Python script, or both). Save the brief alongside the deliverable at the agreed location (ad-hoc default: `analytics/ad_hoc/<YYYY-MM-DD>_<brief_id>_brief.yaml`).
5. **Results checkpoint. HARD GATE.** Present the executed results and the robustness checks to the user and STOP. Do not spawn any reviewer until the user gives explicit approval to proceed. The user may redirect scope here, which is cheaper than a wasted review cycle. A clean-looking result or an eager requester is not a reason to skip this gate.
6. **Review.** Spawn the `product-data-scientist` (methodology + interpretation) and `product-manager` (usefulness) reviewers as sub-agents on the executed analysis.
7. **Capture calibration (two tracks).** Run the closing calibration pass (`.claude/skills/win-analytics-process/references/calibration.md`).
   - **Track 1: data calibration (default, on).** Reusable data facts, joins, gotchas, metric definitions, coverage, canonical-list additions. Route to the owning `win-analytics-knowledge` doc. Propose the exact edit; apply only after explicit human approval.
   - **Track 2: process calibration (default, OFF).** Changes to the framing routine, the executor / `analytics/lib`, the reviewer agents, or the process skill itself. On a standard run, do NOT propose or apply these — park them in the log under a "Process-design candidates" heading, state in the closing summary that process calibration was OFF (not run), and offer to rerun in process-design mode. Act on Track 2 only if the user has explicitly opted into process-design mode this session.
   - No core or shared file is edited without explicit human approval. Approved edits follow the calibration branch/PR convention in `.claude/skills/win-analytics-process/references/calibration.md`.
8. **(Opt-in) Profile this run.** Only if the user opted in at Step 1: run `python3 analytics/diagnostics/pipeline_profiler.py <this session's transcript>` — the most-recently-modified `*.jsonl` in `~/.claude/projects/<this-project>/`. It is stdlib-only (no env needed) and appends a per-stage time/token + `Decisions` entry to the gitignored `analytics/diagnostics/logs/profile_log.md` for later review. Default is to skip. See `analytics/diagnostics/baseline_question.md` for the reusable benchmark question + arm protocol.

Seed steps 2–7 (plus Step 8 if profiling was opted into at Step 1) as a TodoWrite list at the start so the staged flow (including the results checkpoint and the two calibration tracks) is not skipped.

## Troubleshooting

- Databricks auth / token-refresh error → the profile's OAuth token is stale; run `databricks auth login --profile <profile>` to refresh. Also confirm `DATABRICKS_HTTP_PATH` is set.
- The orchestrator jumps straight to an answer → it skipped framing; restart from step 2 and seed the to-do.
- A concept resolves to several candidate metrics → resolve it through `.claude/skills/win-analytics-knowledge/references/canonical_metrics.md`, which is the governed registry.
