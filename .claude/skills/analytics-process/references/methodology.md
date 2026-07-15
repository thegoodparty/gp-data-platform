# Methodology

Part of the **analytics-process** skill. Scoping + verifying an analysis. This doc owns
the product-agnostic discipline; the *product's* defaults — resolved scoping decisions,
default cohorts, and the working-set builder — live in the product knowledge skill's
`methodology_defaults.md` (Win: [methodology_defaults.md](../../win-analytics-knowledge/references/methodology_defaults.md)).
Load both: the checklist below tells you *what to settle*; the defaults doc tells you the
product's settled answers.

## Product defaults (pointer)

Before scoping, open the product's `methodology_defaults.md` for:

- the resolved scoping decisions (unit of analysis, outcome variable, windows, baseline);
- the default cohort per analysis type — always name the cohort in the analysis title and headline so consumers know which population is being characterized;
- the working-set builder and its column caveats.

## Scoping checklist for a new analysis

Before writing any code:

1. **What's the question?** Phrase it as a one-sentence hypothesis. Identify whether the outcome is binary win/loss, time-to-event, funnel completion, or descriptive.
2. **What's the unit of analysis?** Candidacy / user / campaign-version / event. Mismatching grain is the most common methodological bug.
3. **What's the time window?** Apply at the right grain. Election-cycle vs engagement-feature windows may differ.
4. **What's the comparison baseline?** Cohort A vs cohort B requires both to be in scope.
5. **What confounders matter?** Office level, ICP, Pro, incumbency, opponent count. List explicitly.
6. **What does success look like?** A specific number / chart you want to produce, OR a specific question to answer.

If any of these is ambiguous, surface it to the analytics owner via `AskUserQuestion` (or equivalent) BEFORE writing code. Do not list as "open questions" in the deliverable — that wastes a review cycle.

## Project folder pattern (scout-project flavor)

For multi-week scout projects with reusable inventory, create the full structure:

```
analytics/projects/<project_name>/
  CLAUDE.md          (optional, project-local AI guidance)
  INVENTORY.md       (gitignored locally; durable home is the ClickUp product-analytics doc)
  SESSION_NOTES.md   (gitignored, local-only running journal)
  notebooks/
    <project>.ipynb  (the working analysis notebook)
```

The notebook should be set up with a parameterized cycle window at the top so re-running under a different scope is trivial. See `analytics/projects/win_outcomes_scout/notebooks/inventory_queries.ipynb` for the verification-notebook pattern.

## Lightweight analysis pattern (ad-hoc flavor)

For single-notebook ad-hoc analyses (one question, one notebook, no reusable inventory), use the lighter shape:

```
analytics/ad_hoc/
  <YYYY-MM-DD>_<slug>.ipynb
  <YYYY-MM-DD>_<slug>_brief.yaml
```

Date-prefixed filenames are required (chronological sortability). No INVENTORY.md / SESSION_NOTES.md scaffolding. The brief sits alongside the notebook so the framing is retrievable after the fact.

## Choosing the save location

Decide ad-hoc vs project at framing time (the framing routine surfaces this; see [framing.md](framing.md)):

- **`analytics/ad_hoc/`** when the work is a single one-off question, one notebook, no reusable inventory, not expected to be revisited.
- **`analytics/projects/<name>/`** when the work is multi-week, carries reusable inventory, spans multiple notebooks, or will be revisited under different scopes.

When unsure, default to `ad_hoc/`: promoting an ad-hoc analysis into a project later is cheap, the reverse is not.

**If the chosen directory does not exist in the repo, do not silently scaffold it.** Creating `analytics/ad_hoc/` or a new `analytics/projects/<name>/` establishes repo structure, so surface it and confirm with the user before creating it.

## Reusable building blocks — build the working set once, slice it

Don't rebuild cohort + engagement logic from scratch each analysis. **Default executor step:** build the product's consolidated per-user working set once with its committed builder, then slice every cut from it in pandas (the build-once-slice-many rule). Carrying the slice dimensions up front makes re-cuts free — see the amend path in [brief-schema.md](brief-schema.md). The builder, its allowlist source, and its column caveats are product facts: see the product's `methodology_defaults.md` (Win: `analytics/lib/win_analysis.py`, documented there) before using any of its columns as a canonical cohort.

## Notebook sync workflow

Sync local notebooks to Databricks for execution:

```bash
./analytics/scripts/sync.sh projects/<project_folder>     # watcher: local → Databricks
./analytics/scripts/pull.sh projects/<project_folder> <notebook_name>   # reverse: Databricks → local
```

Both honor a `DATABRICKS_WORKSPACE_USER` env var. Default scratchpad: `/Workspace/Users/${USER}/scratchpad/`. Notebook filenames must be unique across all `analytics/projects/*/notebooks/` since they land flat in scratchpad. (`scripts/` is gitignored and per-contributor; if your local `sync.sh`/`pull.sh` hardcode `analytics/<arg>`, update them to `analytics/projects/<arg>` after this move.)

See `analytics/projects/win_churn/WORKFLOW.md` for the full local-canonical / Workspace-execution rationale.

## Ad-hoc query pattern

For inspection / scoping queries, use `dbt show --inline` (from `dbt/project/`) directly against prod paths:

```bash
dbt show --inline "SELECT ... FROM goodparty_data_catalog.<schema>.<table> WHERE ..." --limit 50
```

Per project memory: hit `goodparty_data_catalog.*` directly. `ref()` can resolve to stale dev artifacts. Prod schemas: `dbt` (staging + intermediates; the legacy `dbt_staging` schema is being retired, everything is consolidating into `dbt`), `mart_analytics` / `mart_civics` (marts), `model_predictions` (MLflow outputs).

For larger query results that exceed `dbt show` truncation, run SQL through the profile-auth helper `analytics/lib/databricks_conn.py` (`run_query(sql) -> DataFrame`, authenticates via the `~/.databrickscfg` profile, set up with `databricks auth login`). Pull-script pattern in `analytics/projects/win_outcomes_scout/notebooks/_pull_amplitude_universe.py`.

## Binning conventions

When binning a continuous engagement or outcome metric:

- Prefer pre-registered bins in the brief, with anchors tied to interpretable thresholds (e.g., funnel-stage boundaries: 0 active weeks = didn't return, 1-3 = light user, etc.).
- If bins are chosen after viewing the distribution, document this explicitly in the notebook and report sensitivity to bin choice.
- Report Wilson 95% CIs where they inform the read — when a bin is small enough to be over-read (see the N<30 flag below) or when a difference between bins or periods is being claimed — so readers can distinguish real differences from sampling noise. Skip them on large-N descriptive cuts where the interval is trivially tight and adds only clutter; when in doubt, include them.
- Flag any bin with N<30 as small-sample.

## Verification protocol

A scout / analysis is "done" when:

1. Every numeric claim in the deliverable points to a notebook cell (or query) that reproduces it.
2. Cycle / scope parameters are parameterized so the work re-runs under a different scope.
3. Open scoping questions are RESOLVED (not deferred to the reader).
4. Reusable insights (joins, gotchas, source-system precedence rules) that emerged are routed through the calibration pass (step 5), not written directly into the reference docs.
5. **Calibration pass done.** Findings that pass the promotion test are triaged into a dated `CALIBRATION_<date>.md`, observations below that bar are appended to the shared `analytics/runbook/CANDIDATES.md` ledger, OR you have explicitly recorded that none were needed — the expected outcome of most runs (see [calibration.md](calibration.md)). The closing step is required, not optional — it's how the process self-corrects across runs.

## Source pointers & references

Product-specific source pointers (contributing project scouts, authoritative dbt model docs) live in the product's `methodology_defaults.md`.

**Conventions:**
- `CLAUDE.md` (repo root) — multi-venv reality + don't-disable-pre-commit rules.
- `dbt/project/CLAUDE.md` — dbt-development conventions (do not invoke dbt via poetry; use `dbt show --inline`; branch / commit naming; etc.).
- `ai-rules/` (submodule) — broader AI-assisted-development rules.

**External tickets / documentation:**
- ClickUp tickets are tagged `DATA-XXXX` and surfaced in commit messages + branch names. Search by ticket ID.
- For data platform issues that span multiple subprojects, prefer linking the ticket / PR over re-explaining the issue here.

## Cross-references

- [pipeline.md](pipeline.md) — the pipeline topology and stage handoffs.
- [brief-schema.md](brief-schema.md) — the framing→execution brief contract.
- [calibration.md](calibration.md) — the closing calibration step.
- Knowledge skill (`.claude/skills/win-analytics-knowledge/references/`) — the data facts these methods operate on.
