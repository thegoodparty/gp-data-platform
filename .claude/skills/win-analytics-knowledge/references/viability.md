# Viability Score 2.0 reference

Part of the **win-analytics-knowledge** skill. Race-difficulty stratification. Owns the detail
for **Viability Score 2.0** defined in [canonical_metrics.md](canonical_metrics.md).

## Quick reference

- **Business context:** the electoral-viability-for-winning score — how likely a candidate is to
  win their race, used to stratify analyses by race difficulty. Not a lead-routing score
  (deliberately excluded from lead scoring).
- **Source of truth:** `mart_civics.candidacy_scored` — columns `viability_score` (0.0–5.0) and
  `score_viability_automated` (5-band label). One row per `gp_candidacy_id`, same grain and
  row set as `candidacy`.
- **Never read `candidacy.viability_score`.** It is the archive-only gap-filler that feeds
  `candidacy_scored`'s coalesce (~1.1% of Nov-2025 rows, as of 2026-08). Analyst reads belong on
  `candidacy_scored`; `users_win_candidacy` and `leads_win_candidacy` already surface it.
- **Producer:** `int__civics_viability_scoring` (dbt Python model) — a 5-model MLflow waterfall
  scoring every candidacy the features allow; per-row provenance columns `scoring_model` and
  `model_version` live on this intermediate (not on the mart).

## Routing triggers

- IF you need the governed definition / bands → [canonical_metrics.md](canonical_metrics.md).
- IF you need viability on a Win population → `users_win_candidacy.viability_score` (already
  reads `candidacy_scored`), or join `candidacy_scored` via `product_campaign_id` +
  election-date; see [joins.md](joins.md).
- IF you're stratifying → use the 5-band label. The distribution is NOT bimodal under the
  waterfall scorer (see Score distribution); deciles remain a poor choice because mass
  concentrates unevenly across bands.
- IF coverage looks low in your population → check the seats gate below before assuming the
  score is missing at random. Missingness is structural, not random.

## What it is

An MLflow-registered logistic model family (`goodparty_data_catalog.model_predictions.*`),
trained by the research team, wrapped by the scorer as `round(5 * P(win), 2)`:

- **`viability_score`** — 0.0 to 5.0.
- **`score_viability_automated`** — 5-band label:

| Band | Threshold |
|---|---|
| `No Chance` | `< 1.0` |
| `Unlikely to Win` | `1.0 – < 2.0` |
| `Has a Chance` | `2.0 – < 3.0` |
| `Likely to Win` | `3.0 – < 4.0` |
| `Frontrunner` | `≥ 4.0` |

## The waterfall (which model scores a row)

Best available wins via coalesce; `scoring_model` on the intermediate records the winner:

| # | Model | Drops | Notes |
|---|---|---|---|
| 1 | `viabilitywithopponentdata` | — (all 9 features) | Strongest; needs incumbency, open-seat, opponent count |
| 2 | `viabilitywithoutopenseat` | `open_seat` | |
| 3 | `viabilitynoopponentdata` | `open_seat`, `log_n_losers` | Still needs `is_incumbent` |
| 4 | `viabilitynoincumbency` | `open_seat`, `is_incumbent` | Still needs `log_n_losers` |
| 5 | `viabilitynocandidatedatahs` | all candidate-level features | Weakest; race/office features only |

Features: `multi_seat`, `partisan_contest`, `is_unexpired` (hardcoded false), `office_type_woe`,
`state_woe`, `level_woe`, `is_incumbent`, `open_seat`, `log_n_losers`. The WoE lookups map
unknown/NULL categories to `Other`, `partisan_contest` defaults, so **the only feature that can
hard-block a score is `multi_seat`** — derived from `election.seats_available`, with a
trust-gated BallotReady fallback (`int__civics_viability_seats_fallback`, race-then-position
tiers) where the election link supplies none. A row with neither source → no score from ANY
model. Fallback-seated rows compute `multi_seat` only (no opponent count), so they score via
the no-opponent-data models. Scores are heterogeneous in fidelity: on Nov-2025, model 1 averages ~3.1
while model 5 averages ~1.5 — slice by `scoring_model` when comparing subpopulations whose
model mix differs.

## Coverage (as of 2026-08)

Coverage is gated by the candidacy→election link and `seats_available`:

| Population | Coverage |
|---|---|
| Nov-2025 candidacies, `candidacy_scored` | ~80% |
| Nov-2025 Win campaigns with a result (via `users_win_candidacy`) | ~96% post seats-fill (was ~63%); check `scoring_model` and the fallback's `seats_source` before treating filled rows like full-model scores |
| Legacy `candidacy.viability_score`, Nov-2025 | ~1.1% (do not use) |
| Candidacies linked to an election with `seats_available > 0` | ~100% |

The residual missing mass is structural: candidacies with no BallotReady route at all (no
position key, or trust-gate rejects). The fallback recomputes from BR staging every run, so
fills track BR data improvements automatically.

## Score distribution (as of 2026-08, waterfall scorer)

No Chance 26% / Unlikely 23% / Has a Chance 18% / Likely to Win 5% / Frontrunner 27%. The old
"86% in the extremes" bimodality claim described the retired TechSpeed-era scorer — under the
waterfall the middle bands carry real mass, but unevenly (`Likely to Win` is thin). Stratify on
the 5-band label; avoid deciles.

## Gotchas

- **Legacy column trap:** `candidacy.viability_score` is not the score. It survives only as
  `candidacy_scored`'s archive gap-filler.
- **Scores drift between builds:** the scorer loads the LATEST registered MLflow version of each
  model and the WoE tables are mutable lookups, so a rebuild can shift scores without any dbt
  change. Pin analyses to a snapshot date; `model_version` on the intermediate records what ran.
- **Training vintage / leakage:** model 1 trained 2025-09-30, model 5 trained 2025-06-06 (MLflow
  registry runs) — both pre-date the Nov-2025 elections, so Nov-2025 outcome analyses controlled
  on viability are leakage-safe against those artifacts. Re-verify vintage before using the score
  as a control on later cycles (a retrain could postdate them).
- **Model-mix heterogeneity:** populations that differ in link quality differ in scoring model.
  Report the `scoring_model` mix whenever comparing viability across cohorts.

## Common query patterns

Win campaigns with viability (the join `users_win_candidacy` already implements):

```sql
select c.campaign_id, cs.viability_score, cs.score_viability_automated
from mart_analytics.campaigns c
left join mart_civics.candidacy_scored cs
  on c.campaign_id = cs.product_campaign_id
 and (c.election_date = cs.general_election_date or c.election_date is null)
```

Provenance-aware read (which model produced each score):

```sql
select cs.gp_candidacy_id, cs.viability_score, sc.scoring_model, sc.model_version
from mart_civics.candidacy_scored cs
left join dbt.int__civics_viability_scoring sc using (gp_candidacy_id)
```

## Cross-references

- [canonical_metrics.md](canonical_metrics.md) — governed definition + bands.
- [joins.md](joins.md) — campaign→candidacy join mechanics.
- [segmentation.md](segmentation.md) — viability as a stratification dimension.
- [outcomes.md](outcomes.md) — the outcome definitions viability-controlled reads pair with.
