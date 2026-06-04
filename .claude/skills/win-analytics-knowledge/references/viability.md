# Viability Score 2.0 reference

Part of the **win-analytics-knowledge** skill. Race-difficulty stratification. Owns the detail
for **Viability Score 2.0** defined in [canonical_metrics.md](canonical_metrics.md).

## Quick reference

- **Business context:** the politics-team electoral-viability-for-winning score — how likely a candidate is to win their race, used to stratify by race difficulty.
- **Entity grain:** one score per candidacy (`gp_candidacy_id`), keyed upstream by `techspeed_candidate_code`.
- **Standard hygiene filter:** a row is scored only if **every** model feature is populated; missing one → NULL rating.

## Routing triggers

- IF you need the governed definition / bands → [canonical_metrics.md](canonical_metrics.md) (full threshold table below).
- IF you need a join path that survives a mart rebuild → join `int__techspeed_viability_scoring` via `techspeed_candidate_code`, NOT `candidacy.viability_score` (see Code/data discrepancy).
- IF you're bucketing for stratification → use the 5-band label or bimodal-aware quintiles, NOT deciles (see Score distribution).
- IF you're plotting calibration → flag the leakage risk below.

## What it is

**Viability Score 2.0 is the politics-team electoral-viability-for-winning score** — produced internally by `int__techspeed_viability_scoring.py` (`dbt/project/models/intermediate/techspeed_to_hubspot/`) using an MLflow-registered sklearn classifier:

```
goodparty_data_catalog.model_predictions.ViabilityWithOpponentData
```

NOT a BR-sourced field. NOT a lead-routing scorer. The model wraps `predict_proba` and surfaces two columns:

- **`viability_rating_2_0`** — `round(5 * probability_of_winning, 2)`. Range 0.0 to 5.0.
- **`score_viability_automated`** — 5-band categorical label:

| Band | Threshold |
|---|---|
| `No Chance` | `< 1.0` |
| `Unlikely to Win` | `1.0 – < 2.0` |
| `Has a Chance` | `2.0 – < 3.0` |
| `Likely to Win` | `3.0 – < 4.0` |
| `Frontrunner` | `≥ 4.0` |

## Features (model inputs)

- `is_incumbent` (bool)
- `open_seat` (bool)
- `multi_seat` (0/1)
- `partisan_contest` (0/1)
- `is_unexpired` (always False in this pipeline)
- `log_n_losers` — `log(n_candidates - n_seats)` floored at `log(0.001)` for uncontested
- `state_woe`, `level_woe`, `office_type_woe` — Weight-of-Evidence-encoded categoricals (encodings in `goodparty_data_catalog.model_predictions.viability_br_{state,level,office_type}_woe`)

**A row gets a score only if EVERY feature is populated.** Missing one feature → NULL rating. This is why BR-only candidacies are spotty — BR doesn't supply `n_seats`, `is_incumbent`, or `n_candidates` for all rows.

## Where the score lives (3 distinct tables)

| Table | Key | Coverage | Use |
|---|---|---|---|
| `goodparty_data_catalog.dbt.int__techspeed_viability_scoring` | `techspeed_candidate_code` | ~99.7% rated | Model output. **Forward-stable join path.** |
| `goodparty_data_catalog.dbt.stg_model_predictions__viability_scores` | HubSpot company `id` | 100% rated | Separate scoring run (HubSpot-keyed features). Used by `int__civics_candidacy_2025` for the 2025 archive. |
| `goodparty_data_catalog.mart_civics.candidacy.viability_score` | `gp_candidacy_id` | Varies by source bucket (see below) | The mart-level field surfaced into `users_win_candidacy`. |

## Coverage in `mart_civics.candidacy` by source bucket

| source bucket | coverage % |
|---|---:|
| 2026+ TS-only (no BR match) | ~97.8% |
| 2026+ BR-matched (TS+BR) | ~56.3% |
| 2025 archive (HubSpot) | ~4.4% |
| 2026+ gp_api-only | 0% |
| 2026+ ddhq-only | 0% |

For Win-filtered analyses (joining through `users_win_candidacy`), coverage drops further to ~13% — Win users come in via `gp_api` and only get a score if the ER crosswalk lands them on a TS or TS+BR candidacy.

## Score distribution is bimodal

86% of scored candidates land in `Frontrunner` or `No Chance`; only 14% in the three middle bands. Consistent with a model dominated by incumbency + opponent-count features. **Don't bucket into deciles for stratification — use the 5-band label or quintiles aligned to the bimodal shape.**

## Gotchas

### ⚠ Code/data discrepancy (flag for the data platform team)

The current SQL files for `int__civics_candidacy_ballotready.sql` and `int__civics_candidacy_techspeed.sql` (both in main) hardcode `cast(null as float) as viability_score`. Yet the prod intermediate tables have scores populated. Two possible explanations:

1. The prod intermediate hasn't been rebuilt since the null hardcode landed (2026-02-09, commit `61120ab1`). A future rebuild would drop ~58k scores from the mart.
2. There's an unobserved code path (notebook, Airflow job, hand-merge) populating the score post-hoc.

**Forward-stable analysis pattern:** join `int__techspeed_viability_scoring` directly via `techspeed_candidate_code` instead of relying on `candidacy.viability_score`. (Symptom table: [gotchas.md](gotchas.md).)

### Calibration leakage risk

`int__techspeed_viability_scoring` is materialized as a `table` and re-computes on each `dbt run`. No historical snapshot. If the MLflow model is retrained against post-election data, calibration on 2025 outcomes is potentially contaminated. Flag this on any calibration plot.

## Common query patterns

### Back-scoring NULL rows

Even where `viability_score` is NULL, the underlying features are partially available:
- `is_incumbent` (TS-sourced; sparse on BR-only)
- `is_open_seat` (BR > TS > DDHQ)
- `is_partisan`
- `n_seats` / `n_candidates`: reconstructible from `candidacy_stage` counts per `gp_election_stage_id`
- WoE encodings: stable lookup tables

In principle, the score can be back-scored for additional candidacies by joining feature rows against the existing MLflow model.

## Cross-references

- [canonical_metrics.md](canonical_metrics.md) — governed definition + bands.
- [joins.md](joins.md) — the forward-stable `techspeed_candidate_code` join.
- [segmentation.md](segmentation.md) — viability as a stratification dimension alongside ICP/office.
