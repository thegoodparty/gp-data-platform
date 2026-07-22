# Win methodology defaults

Part of the **win-analytics-knowledge** skill. The Win-product defaults consumed by the
process skill's [methodology.md](../../analytics-process/references/methodology.md):
that doc owns the product-agnostic discipline (scoping checklist, folder patterns, binning,
verification protocol); this one owns what is true *for Win* — the resolved scoping
decisions, the default cohorts, and the working-set builder.

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
| Engagement vs outcome | Onboarded cohort | **onboarding dashboard viewed** — the canonical Onboarded metric ([canonical_metrics.md](canonical_metrics.md)); recompute from the raw event, never the stored onboarding flags ([engagement.md](engagement.md)) |
| Outreach intensity vs outcome | Activated cohort (sent ≥1 outreach) | `is_activated = TRUE` |
| Funnel / dropoff analyses | Full registered population | `users_win_candidacy` filtered to `is_latest_version AND NOT is_demo` |
| Active candidates OKR reporting | Active candidates (trailing 30d) | `is_active_candidate_30d = TRUE` (or recompute anchored to a target date) |

Always name the cohort in the analysis title and headline so consumers know which population is being characterized. "Among onboarded Win candidates..." not "Among Win candidates..."

## The working-set builder

The committed package `analytics/lib/win_analysis.py` holds the canonical Win event-family allowlist (`win_event_predicate`), a `build_win_working_set(run_query, cohorts, ...)` that returns one consolidated per-user `cohort × engagement` DataFrame carrying the standard slicing dimensions from [segmentation.md](segmentation.md), and `wilson`. Build that one working set first, then slice every cut from it in pandas (the build-once-slice-many rule in the process skill's methodology.md). Carrying the slice dimensions up front makes re-cuts (e.g. ICP vs not) free — see the amend path in the process skill's brief-schema.md. The event classification is sourced from `int__amplitude_event_catalog` (see [engagement.md](engagement.md) — no separate hand-maintained allowlist to keep in sync).

> **Column caveat — don't use the helper's `onboarded` column as the Onboarded cohort.** It keys on `onboarding_complete`, a new-flow-blind flag (FALSE for every post-2026-05-07 user; [engagement.md](engagement.md) owns the mechanism), so it is not the canonical **Onboarded** cohort above. Recompute the 14-day dashboard-view logic — the helper's `dash_viewed` is dashboard-view-*ever* in the pre-anchor window, not within-14-days-of-creation — and use `Onboarding - Candidate Pledge Completed` for cross-cutover completion.

## Reviewer doc pointers (used by the dispatch template)

When the reviewer dispatch template in the process skill's pipeline.md asks for the product's reviewer doc pointers, Win's are:

- `product-data-scientist`: this skill's [viability.md](viability.md) and [gotchas.md](gotchas.md), plus the process skill's methodology.md.
- `product-manager`: this skill's [segmentation.md](segmentation.md), plus the process skill's methodology.md.

## Source pointers

**Project scouts that contributed insights:**
- **`analytics/projects/win_outcomes_scout/INVENTORY.md`** (DATA-1935) — Win product × electoral outcomes data scout. Seeded most of this skill's content, especially `outcomes.md` through `viability.md`. Source 7 in the inventory carries the full HubSpot survey detail (per-survey submission counts, response distribution by ICP/Win bucket, the verified Option 1/2 mapping evidence).
- **`analytics/projects/win_churn/`** (DATA-1924) — Win churn modeling. Engagement-as-outcome counterpart. Source for some of the multi-cycle and pre-2023-12-10 gotchas.

**Authoritative dbt model docs:**
- `dbt/project/models/marts/civics/m_civics.yaml` — column descriptions for candidacy / candidate / candidacy_stage / election / election_stage.
- `dbt/project/models/marts/analytics/m_analytics.yaml` — for the analytics mart.
- `dbt/project/models/intermediate/amplitude/int__amplitude.yaml` — Amplitude intermediate columns + tests.
