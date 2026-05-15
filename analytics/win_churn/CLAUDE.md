# Win product churn modeling

This folder hosts the first churn/retention modeling project for GoodParty's Win product (candidate-facing tool). Tracked in ClickUp as **DATA-1924** ("Churn Modeling for Win Product"; will be converted to an epic).

## Where to look first

- `SESSION_NOTES.md` — running journal of decisions, findings, and dead ends. **Read this before making changes** — it is the canonical context source for this project.
- `TRAINING.md` (Phase 2 deliverable, not yet written) — pointer to the Databricks training notebook, MLflow experiment URL, current model version, and retraining cadence.

## Where the code lives

- Phase 1 marts: built initially in the developer's dev schema; promoted to `dbt/project/models/marts/analytics/users_win_state_weekly` etc. after notebook validation.
- Phase 1 weekly-activity rollup: `dbt/project/models/intermediate/int__amplitude_win_activity_weekly.sql`.
- Phase 2 feature mart + Phase 3 scoring model: `dbt/project/models/intermediate/win_churn/`.
- Phase 3 published scores view: `dbt/project/models/marts/analytics/users_win_churn_scores.sql`.
- Training notebooks live in Databricks Workspace, not committed.

## Critical leakage trap

`users_win_base.last_dashboard_viewed_at` and the `is_active_candidate_*` flags are computed against `current_date` and **cannot** be used as features in any point-in-time feature mart. Build features from the weekly activity rollup with explicit `asof_date` semantics.

## Population filters (applied at Phase 1 mart layer)

- US only (`registration_country = 'United States'`)
- Exclude demo-only users (`not is_demo_only`)
- Exclude pre-Amplitude registrations (`registered_at >= 2023-12-10`) for predictive modeling

## Schema strategy

New dbt models materialize in the dev schema first; promotion to `marts/analytics/` is a follow-up PR after validation. See SESSION_NOTES.md for the validation checklist per model.
