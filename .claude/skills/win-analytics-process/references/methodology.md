# Methodology

Part of the **win-analytics-process** skill. Scoping + verifying an analysis.

## Resolved scoping decisions (DATA-1935, 2026-05-27)

These apply by default to Win-product analyses. Document deviations in your project's `SESSION_NOTES.md`.

| Decision | Default |
|---|---|
| Unit of analysis | Candidacy (`gp_candidacy_id`) |
| Outcome variable | `latest_stage_reached + latest_stage_result` from `mart_civics.candidacy` (NOT `general_election_result`) |
| Cycle window | `general_election_date BETWEEN '2025-05-01' AND '2026-12-31'` (adjust per project) |
| Engagement window | Same as candidacy window unless analysis specifically wants trailing features |
| Baseline | All registered Win users; engagement-zero is a valid predictor value (not an exclusion) |
| ICP gating | `icp_office_win` is a slicing dimension, not a filter |

## Default cohorts by analysis type

Different analyses call for different cohorts. The full Win-product registered population includes registered-but-never-active candidates, pre-instrumentation registrants, CRM-sync candidates who never logged in, and other heterogeneous funnel stages. Pooling them produces misleading correlations — for example, raw engagement-vs-win-rate looks *inverse* (more engagement → lower win rate) because the 0-engagement bin is dominated by candidates who never used the product at all, not by candidates who tried-and-failed to engage.

| Analysis type | Default cohort | Source flag |
|---|---|---|
| Engagement vs outcome | Onboarded cohort | **onboarding dashboard viewed** = dashboard view within 14d of `user_created_at`, recomputed from the raw event (NOT the new-flow-blind `has_completed_onboarding_flow` / `is_onboarded` flags — see the knowledge skill's [engagement.md](../../win-analytics-knowledge/references/engagement.md) and [gotchas.md](../../win-analytics-knowledge/references/gotchas.md)) |
| Outreach intensity vs outcome | Activated cohort (sent ≥1 outreach) | `is_activated = TRUE` |
| Funnel / dropoff analyses | Full registered population | `users_win_candidacy` filtered to `is_latest_version AND NOT is_demo` |
| Active candidates OKR reporting | Active candidates (trailing 30d) | `is_active_candidate_30d = TRUE` (or recompute anchored to a target date) |

Always name the cohort in the analysis title and headline so consumers know which population is being characterized. "Among onboarded Win candidates..." not "Among Win candidates..."

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

## Reusable building blocks — build the working set once, slice it

Don't rebuild cohort + engagement logic from scratch each analysis. The committed package `analytics/lib/win_analysis.py` holds the canonical Win event-family allowlist (`win_event_predicate`), a `build_win_working_set(run_query, cohorts, ...)` that returns one consolidated per-user `cohort × engagement` DataFrame carrying the standard slicing dimensions from the knowledge skill's [segmentation.md](../../win-analytics-knowledge/references/segmentation.md), and `wilson`. **Default executor step:** build that one working set first, then slice every cut from it in pandas (codifies the build-once-slice-many rule). Carrying the slice dimensions up front makes re-cuts (e.g. ICP vs not) free — see the amend path in [brief-schema.md](brief-schema.md). The event classification is sourced from `int__amplitude_event_taxonomy` (see the knowledge skill's [engagement.md](../../win-analytics-knowledge/references/engagement.md) — no separate hand-maintained allowlist to keep in sync).

> **Column caveat — don't use the helper's `onboarded` column as the Onboarded cohort.** `build_win_working_set`'s `onboarded` column keys on `onboarding_complete`, which is the **new-flow-blind** flag (FALSE for every post-2026-05-07 user). It is **not** the canonical **Onboarded** cohort from the default-cohorts table above (dashboard view within 14 days of account creation). For that cohort, recompute the 14-day dashboard-view logic — the helper's `dash_viewed` is dashboard-view-*ever* within the pre-anchor window, not the 14-day-of-creation window — and for cross-cutover onboarding *completion* use `Onboarding - Candidate Pledge Completed`. Same new-flow-blindness documented in the knowledge skill's [engagement.md](../../win-analytics-knowledge/references/engagement.md) and [gotchas.md](../../win-analytics-knowledge/references/gotchas.md), and in the lib's own README.

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

For larger query results that exceed `dbt show` truncation, use the `databricks-sql-connector` Python client via the global env vars (see user-level CLAUDE.md). Pattern in `analytics/projects/win_outcomes_scout/notebooks/_pull_amplitude_universe.py`.

## Binning conventions

When binning a continuous engagement or outcome metric:

- Prefer pre-registered bins in the brief, with anchors tied to interpretable thresholds (e.g., funnel-stage boundaries: 0 active weeks = didn't return, 1-3 = light user, etc.).
- If bins are chosen after viewing the distribution, document this explicitly in the notebook and report sensitivity to bin choice.
- Always report Wilson 95% CIs alongside point estimates so readers can distinguish real differences from sampling noise.
- Flag any bin with N<30 as small-sample.

## Verification protocol

A scout / analysis is "done" when:

1. Every numeric claim in the deliverable points to a notebook cell (or query) that reproduces it.
2. Cycle / scope parameters are parameterized so the work re-runs under a different scope.
3. Open scoping questions are RESOLVED (not deferred to the reader).
4. The reference docs are updated with any reusable insights (joins, gotchas, source-system precedence rules) that emerged.
5. **Calibration pass done.** Findings that should update the agents or docs are triaged into a dated `CALIBRATION_<date>.md` (see [calibration.md](calibration.md)), OR you have explicitly recorded that none were needed. This is a required closing step, not optional — it's how the process self-corrects across runs.

## Source pointers & references

**Project scouts that contributed insights:**
- **`analytics/projects/win_outcomes_scout/INVENTORY.md`** (DATA-1935) — Win product × electoral outcomes data scout. Seeded most of the knowledge-skill content, especially `outcomes.md` through `viability.md`. Source 7 in the inventory carries the full HubSpot survey detail (per-survey submission counts, response distribution by ICP/Win bucket, the verified Option 1/2 mapping evidence).
- **`analytics/projects/win_churn/`** (DATA-1924) — Win churn modeling. Engagement-as-outcome counterpart. Source for some of the multi-cycle and pre-2023-12-10 gotchas.

**Authoritative dbt model docs:**
- `dbt/project/models/marts/civics/m_civics.yaml` — column descriptions for candidacy / candidate / candidacy_stage / election / election_stage.
- `dbt/project/models/marts/analytics/m_analytics.yaml` — for the analytics mart.
- `dbt/project/models/intermediate/amplitude/int__amplitude.yaml` — Amplitude intermediate columns + tests.

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
