# Legit candidate predictor (DATA-2225, epic DATA-2250)

Scores Win signups on how likely their campaign is to be corroborated by a ballot data vendor (BallotReady, DDHQ or TechSpeed), before that confirmation arrives. BallotReady typically lags signup by about 40 days, so the score fills that gap.

This directory is the code only. The parquet caches under `datasets/`, generated reports (`profiles/`, `intake/`), the MVP notebook and the full working notes are not in git; `SESSION_NOTES.md` is a condensed version of the notes (decisions, measured results, failed ideas, pitfalls). Every cache is rebuilt by a `build_*.py` script from the warehouse.

## What the score means

- **Target is corroboration, not realness.** A low score means "unlikely to be confirmed on a ballot", never "not a candidate". The bottom decile is about 0.5% corroborated but an estimated 44 to 52% real people (PU correction, `pu_correction.py`).
- So the bottom band can never be an exclusion band. Allowed wording: "unconfirmed, deprioritise". Forbidden: "not a candidate", "exclude", "do not contact".
- **Ranking transfers, absolute values do not.** Base rate is driven by lead time (signup to election). The live population sits a median 421 days out, the training data a median 66 days, with only 19.5% overlap. Scores are therefore served as within-cohort quantile bands (lead-time bucket x office type), not a threshold.

## Current state

| Thing | Where |
|---|---|
| Feature layer (75 serving columns) | dbt, on `main`: `int__win_signup_features`, `int__win_position_features`, `int__win_candidate_incumbency`, `int__l2_person_bridge`, `mart_analytics.win_candidate_features` (DATA-2315, PR #1045) |
| Scores (one run, 2026-08-14) | `model_predictions.win_candidate_scores_20260814`, long-format `win_candidate_scores` + `win_candidate_score_context`, view `win_candidate_scores_latest`; staged in dbt as `stg_model_predictions__win_candidate_scores_latest` |
| Fitted models | MLflow experiment `/Shared/legit_candidate_predictor`. Serving run `0e3e830fa67a4c78aa69d3f6275390b8` (manifest `21ff4ad08ba3`), profile run `38d903714a5c4829bbe8102eabc61779` (manifest `caffc2f61d1e`) |
| Unity Catalog | Being registered as `model_predictions.win_legit_candidate_serving` and `win_legit_candidate_profile`, alias `champion`, from the two runs above (not retrained) |
| Hand-verified labels | `model_predictions.web_verify_verdicts_20260813` (783 rows, DATA-2251) |
| Experiment history | `results_ledger.jsonl` (mirrored to the MLflow experiment) |

Read `model_id` and `mlflow_run_id` off a scored row rather than trusting an id in prose. `model_id` hashes the contents of `mvp_model.py`, so it changes on unrelated edits.

Canonical numbers: pass-B baseline HGB 0.8732 OOF AUC on working set `b04cdd5ac4b1` (20,732 rows). v2 serving composition 0.8819 pooled / 0.8362 decidable / 0.8922 forward.

## Two model tiers

- **Serving**: all features (BR position 32 + gp_user_details 12 + incumbency 7, on top of the baseline).
- **Profile / analysis**: the same model with claim-reserved columns masked (`mvp_model.CLAIM_MASKS`: incumbency, office level, district size, self-reported effort). A variable can't be both a model input and evidence about the group the score defines, so any analysis that wants to say something about one of these uses the profile tier.

## Run order

All commands run from `analytics/`. Each script's docstring has its exact `uv run --with ...` line. Auth is OAuth through the `~/.databrickscfg` profile via `analytics/lib/databricks_conn.py`.

1. `build_working_set.py`: labelled population (completed elections). `build_scoring_population.py` reuses the same SQL with the completion filter flipped, so training and scoring rows can't drift.
2. Feature families: `build_geo.py`, `build_br_position.py`, `build_gp_user_details.py`, `build_incumbency.py` (plus the experimental families: `build_l2_*`, `build_race_priors.py`, `build_derived_wave*.py`, `build_amplitude_scaffold.py`, `build_name_plausibility.py`).
3. `harness.py --freeze` freezes folds for the working set; `--verify` checks the baseline reproduces.
4. Experiments: `exp_family.py --family <names> [--claim X] [--loo] [--manifest] [--ledger]`. Accepted column sets and exclusions live in `mvp_model.FAMILY_SPECS`; use `--family`, not ad-hoc `--exclude` lists. `run_analysis_tiers.sh` runs the analysis-tier variants.
5. `score_population.py`: fits the serving model on the full working set, logs it to MLflow, scores the live population, assigns bands.
6. `load_scores_append.py`: appends the run to the long-format score tables and refreshes the view. (`load_scored_population.py` produced the dated 08-14 table.)

`intake.py` and `profile.py` generate the per-source review docs under `intake/` and `profiles/` (not committed). Every new source gets a profile and an intake review before it enters a model.

## Rules and gotchas worth knowing

- **Label leaks.** `br_candidacy_id` is a label ingredient, so `n_br_candidacies_by_deadline` counts opponents only. Name-derived features leak through entity-resolution name matching (run incumbency with its exclusions; dropping them inflates its lift about 5x).
- **Coverage proxies** are the recurring trap: a feature that mostly says "this person is matchable" looks predictive for the wrong reason. `diag_incumbency_proxy.py` shows how to test one.
- **Check the forward test population before reading a forward delta.** It grows with each rebuild.
- **Derived artifacts go stale silently.** Geo was once built from an older population and reached 0.2% of live rows. Check reach against the target population after any rebuild.
- **L2 / Haystaq:** the full 1,396-column block was screened and adds nothing (`L2_BLOCK_SCREEN_METHOD.md`). A protected-class proxy screen drops columns at a 0.30 posterior shift. L2 vote history and consumer flags are presence-only, so matched-and-null is a real 0.
- **Learners are exhausted.** CatBoost, char n-grams, isolation forest and TabPFN were all washes or negative; the ceiling is the data.
- **In-cycle labels are immature.** At 90 days before the election only 8.5% of eventual corroborations exist. Keep the `general_election_date < current_date()` filter; use the maturity curve (`spike_label_maturity.py`) for base-rate correction, not training.
- **Train/serve skew:** `br_arrivals_missing` is 41% in training and 78% at scoring, because BallotReady has no rosters yet for future elections.

## Open work

- DATA-2565: serve the score for Ops reporting (recurring rescoring job, in-cycle base-rate correction, agreement check against the HubSpot deep-search status).
- DATA-2566: deliver the score into HubSpot for sales. Gated on a decision about per-lead use: the score's composition bias was accepted for aggregate analysis only.
- DATA-2567: result-collection tail triage, using the score as a prior.
- DATA-2288: Feature Store point-in-time features.
- Pass-B baseline has not been re-measured since the geo refresh; redo it at the next re-baseline.
