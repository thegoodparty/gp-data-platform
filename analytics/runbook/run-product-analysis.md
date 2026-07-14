Start a product-analytics session on the right path: refine the question to an approved brief before writing any analysis code, then execute, review, and calibrate. This runbook is the entry point only. The pipeline itself (stages, actors, gates, artifacts) is described once, in `.claude/skills/win-analytics-process/references/pipeline.md`; nothing here restates it.

## Prerequisites

- **Databricks access.** Analytics connects via `analytics/lib/databricks_conn.run_query`, which authenticates with the Databricks SDK profile in `~/.databrickscfg` (set up once with `databricks auth login`; honors `DATABRICKS_CONFIG_PROFILE`). `DATABRICKS_HTTP_PATH` (the SQL warehouse path) must be set; no PAT is used. The live `goodparty_data_catalog` is ground truth.
- **Skills to load.** Read the `win-analytics-process` skill (how to work) and resolve every data concept through the product's knowledge skill (what is true; routed in step 1).

## Steps

1. **Route by product, and ask the profiling opt-in.** Today only the **Win** product is supported: knowledge skill `win-analytics-knowledge`, working-set helper `analytics/lib/win_analysis.py`. (Serve and other products will be added here later.) Also ask the user once whether to **profile this run's pipeline cost** — default **no**; most runs skip it (see step 3). Record the answer.
2. **Seed the staged to-do list and run the pipeline.** Seed the stages (frame → approve framing → execute → results checkpoint → review → calibrate, plus step 3 if profiling was opted in) as a to-do list at the start so no gate is skipped, then step through them as specified in `pipeline.md`. The framing routine is `framing.md`; the brief contract is `brief-schema.md`; the closing calibration pass is `calibration.md` and ends with the candidates-ledger read-back it requires. Both human gates (framing approval; results checkpoint) are hard stops — the gate rules live in `pipeline.md`, and a clean-looking result or an eager requester does not waive them.
3. **(Opt-in) Profile this run.** Only if the user opted in at step 1: run `python3 analytics/diagnostics/pipeline_profiler.py <this session's transcript>` — the most-recently-modified `*.jsonl` in `~/.claude/projects/<this-project>/`. It is stdlib-only (no env needed) and appends a per-stage time/token + `Decisions` entry to the gitignored `analytics/diagnostics/logs/profile_log.md` for later review. Default is to skip. See `analytics/diagnostics/baseline_question.md` for the reusable benchmark question + arm protocol.

## Troubleshooting

- Databricks auth / token-refresh error → the profile's OAuth token is stale; run `databricks auth login --profile <profile>` to refresh. Also confirm `DATABRICKS_HTTP_PATH` is set.
- The orchestrator jumps straight to an answer → it skipped framing; restart from step 2 and seed the to-do list.
- A concept resolves to several candidate metrics → resolve it through the product knowledge skill's `canonical_metrics.md`, which is the governed registry.
