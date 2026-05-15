# Win product churn modeling — session notes

Running journal for the churn modeling project. Append new entries to the end. Use `## YYYY-MM-DD — short title` as session headers. The CLAUDE.md in this folder is the auto-loaded summary; this file is the longform context that gets pulled in deliberately when needed.

## Quick reference

- **ClickUp**: DATA-1924 — https://app.clickup.com/t/86ahgwy12 (to be converted to an epic)
- **Inspiration**: https://www.lennysnewsletter.com/p/how-duolingo-reignited-user-growth
- **ML precedent in this repo**: `dbt/project/models/intermediate/techspeed_to_hubspot/int__techspeed_viability_scoring.py` — MLflow + WoE + dbt-Python pattern to mirror
- **Upstream user-grain mart**: `dbt/project/models/marts/analytics/users_win_base.sql` (note: trailing flags are leak-prone)
- **Upstream activity mart (monthly)**: `dbt/project/models/marts/analytics/users_win_activity.sql` (we will build a weekly analog)
- **MLflow model URI** (Phase 2 placeholder): `goodparty_data_catalog.model_predictions.win_churn_4w`
- **Databricks notebooks** (Workspace, not committed): `win_churn_phase1.ipynb`, `win_churn_train.ipynb` — add the Workspace paths here once created.

## Resolved decisions

1. **Product**: Win (candidates), not Serve. Chosen because the canonical 30d-active OKR is well-defined and the user-grain mart already exists.
2. **Cadence**: weekly. Reflects the ~12-week typical engagement window for Win accounts. Requires building a weekly activity rollup since the existing `users_win_activity` is monthly.
3. **Churn definition (Phase 2 target)**: `P(no activity in next 4 weeks | active in trailing 4 weeks and election_date > asof_date + 4 weeks)`. Right-censor users whose election is within 4 weeks; post-election disengagement is not churn.
4. **State taxonomy (Phase 1)**: engagement-recency axis (new / active / at_risk / dormant_12w / dormant / resurrected / post_election) is separate from lifecycle axis (registered → onboarded → activated → pro). Lifecycle is a feature, not part of the mutually-exclusive recency state.
5. **Population**: US only, exclude `is_demo_only`. Applied at Phase 1 mart level so downstream phases inherit.
6. **Intended action for the score**: analytics-only at launch (Sigma + notebooks). HubSpot operational push deferred — revisit once a confirmed user with an intended action exists.
7. **Schema strategy**: build in dev schema first; promote to `marts/analytics/` only after notebook validation. Phase 3 scoring view is the first artifact that lands in the public mart.
8. **Stack**: Databricks + Python + dbt. Training in Workspace notebook → MLflow registry → dbt Python scoring model following the techspeed precedent.

## Critical pitfalls to design around

- **Leakage from `users_win_base` trailing flags.** `last_dashboard_viewed_at` and `is_active_candidate_*` are computed against `current_date`. They cannot be features. Build point-in-time from the weekly activity rollup with explicit `asof_date`.
- **First-cycle bias.** Training data is dominated by the 2024 election cycle; 2026 may behave structurally differently. Plan to retrain quarterly and watch calibration.
- **Reverse causation.** Pro upgrade and `is_activated` are both predictors and downstream of high engagement. Useful for prediction; misleading for causal storytelling. Flag this when presenting feature importances.
- **Activity-table sparsity.** A missing month in the activity mart could mean "inactive" OR "not yet registered." Always anchor calculations to `users_win_base.registered_at`.
- **Survivorship.** The at-risk-from-active model intentionally excludes users who never activate — that's a separate activation model (possible Phase 4).
- **Train/test split.** Must be temporal (rolling-origin CV), not random K-fold. Random would leak future information back into training.

## Phase status

- **Phase 1 — Cohort framework + univariate analysis**: not started
- **Phase 2 — Predictive model**: not started
- **Phase 3 — Analytics-only scoring view**: not started

## Validation checklist per model

(To be filled in as models are built — track row counts, null rates, parity vs. existing marts, and the dbt-show queries that confirmed each invariant.)

## Session log

### 2026-05-15 — Project kickoff and scaffolding

- Reviewed Duolingo CURR framework writeup; agreed it inspires the cohort/state layer but isn't directly portable (Duolingo's actual approach was descriptive sensitivity analysis, not predictive ML).
- Confirmed the candidate use case differs structurally: bounded by election_date, weekly cadence (not daily).
- Explored the repo's existing assets: `users_win_base`, `users_win_activity`, `int__techspeed_viability_scoring.py`. Identified the leakage trap on the trailing-window flags.
- Resolved the eight decisions listed above.
- Created ClickUp DATA-1924 in the Data Backlog (will be converted to an epic; phase subtasks to follow).
- Scaffolded this folder (`analytics/win_churn/`) on branch `data-1924/win-churn-scaffold`.
- **Terminology resolved**: a "campaign" in the Win product DB is the same concept as a "candidacy" in the civics mart (per `dbt/project/CLAUDE.md` ID mappings — `campaign_id` ↔ `gp_candidacy_id`). So `users_win_base.campaign_count` = number of candidacies per user. Useful as a Phase 2 feature.
- **Next concrete step**: build `int__amplitude_win_activity_weekly.sql` in the dev schema and reconcile it weekly-to-monthly against `users_win_activity`.
