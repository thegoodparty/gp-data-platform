# Legit candidate predictor: condensed session notes

Condensed from the working notes kept 2026-08-04 to 2026-10-15 (DATA-2225, epic DATA-2250). Written for a person or an AI session picking the work up: it records what was decided, what was measured, and what went wrong, so none of it gets re-run or re-learned. `README.md` is the short orientation; read it first.

Conventions used below: AUC triples are **pooled OOF / decidable / forward**. "Decidable" excludes the rows a rule settles on its own (no BR position type). "Forward" trains on earlier signups and tests on later ones. Deltas are against the canonical baseline of the time unless stated.

## Canonical numbers (read these before comparing anything)

- Working set `b04cdd5ac4b1`: 20,732 rows, completed elections, base rate 0.589, filing-deadline coverage 79.4%.
- Pass-B baseline (no product activity): HGB **0.8732 / 0.8240 / 0.8669**, PR-AUC 0.8860.
- v2 serving composition (baseline + BR position 32 + gp_user_details 12 + incumbency 7 = 75 features): **0.8819 / 0.8362 / 0.8922** after the geo refresh.
- Profile tier (`sales_profile` mask, 20 columns removed, 55 left): **0.8725 / 0.8236 / 0.8901**.
- Numbers from before working set `b04cdd5ac4b1` (0.8695, 0.8683 and so on) are not comparable to these.
- Fold noise is roughly +/-0.005 on the forward split (n=1,444) and about +/-0.002 pooled. Treat smaller deltas as washes.

## The label and what the score means

- Label = corroborated: the campaign was matched to a BallotReady, TechSpeed or DDHQ candidacy for a completed election. It is a proxy for "real candidate", not the thing itself.
- Corroboration is a roster process, not a results process: 99.9% of corroborating records exist before election day, median 63 days before. But they arrive late as a maturity curve: 8.5% present at 90 days before the election, 36.5% at 60, 92.5% at 30, 99.4% at 14. So in-cycle rows cannot be labelled yet; keep the `general_election_date < current_date()` filter.
- Base rate is driven by lead time (signup to election). Long-lead signups corroborate at 16 to 33% even on mature elections; short-lead at 60 to 90%.
- Realness is not corroboration. PU correction (`pu_correction.py`) on the hand-verified sample: the bottom score decile is about 0.5% corroborated but an estimated 44 to 52% real. Inside the uncorroborated class the score separates corroboration far better than realness (AUC 0.614 against realness).
- Three realness numbers that get confused (ledger row "PU correction v1"). **Whole working set: 78 to 81% real** (`p_real_floor` 0.7842 to `p_real_ceiling` 0.8054), i.e. the 58.9% corroborated plus the real share of the rest. **Uncorroborated only: 47 to 53% real**, which is (0.7842 - 0.5894) / 0.4106 to (0.8054 - 0.5894) / 0.4106. The 0.437 [0.35, 0.53] is narrower still: the uncorroborated rows *outside the top score bin*, whose top-bin counterpart is 0.767. Never attach 78 to 81% to "uncorroborated users"; that error reached the shared write-up once.
- Consequence, decided: the score is consumed as bands, and the bottom band is never an exclusion band. Allowed meaning "unconfirmed, deprioritise"; forbidden "not a candidate", "exclude", "do not contact".
- Error costs, decided: dropping a real candidate is worse than admitting a noise signup.

## Decisions (still in force)

Population and features
- Office-declared signups only; signups with no office are reported separately.
- Point-in-time allowlist is a hard rule: every feature must be knowable at signup or the filing deadline. No post-signup candidacy fields, results, opponents or viability scores.
- Pass B (no product activity) is the model. Pass A (adds post-signup activity) is diagnostic only.
- One concept, one source: for each concept use the single highest-coverage source; never import the same thing from two systems.
- Entry channels are not user behavior. Any feature that mostly encodes how the account was created (sales import vs web signup, passwordless flow, first-touch attribution) is dropped, even when predictive. Found three times: the knowRun/has_password latent, HubSpot first-touch fields (imports corroborate 70.5% vs 36 to 44% for web channels), and the party field's two casings.
- Protected classes (gender, ethnicity, religion, language, disability) are excluded from both tiers, and so is any column that proxies them at a posterior shift of 0.30 or more, screened label-free on L2 data. The screen cost zero AUC.
- Name-plausibility flags are a-priori rules, never pruned by their label rate.

Training and evaluation
- Training window: everything from 2024 with exponential recency weights, half-life 240 days on the signup clock (`mvp_model.recency_weights()`). Hard cutoffs get worse as they shorten (2025 Q4 only collapsed to 0.7165). 240 rather than 120 days because heavier weighting costs OOF discrimination and the forward difference is inside noise.
- Folds: StratifiedGroupKFold k=5, seed 7, grouped by user, frozen per working-set version (`harness.py --freeze`). Every experiment appends to `results_ledger.jsonl`, mirrored to MLflow.
- Accepted feature families and their exclusions live in `mvp_model.FAMILY_SPECS`. Always run `exp_family.py --family`, never a retyped `--exclude` list.

Two tiers
- A model input cannot be evidence about the group the score defines ("high scorers are mostly incumbents" just restates the rule). So: a serving model on every admissible feature for prioritisation, and a profile model with claim columns masked for any analysis that wants to make claims about them.
- Implemented as a mask, not a second feature set: `mvp_model.CLAIM_MASKS` plus `exp_family.py --claim`, applied to the assembled frame so it reaches baseline columns too.
- Claim set: incumbency, office level, district size, self-reported effort; combined as `sales_profile` with state and office type. Masks were widened by the covariance check (`pos_signature_count` r=0.96 with voter count, `pos_salary_amount` r=0.72).
- Only the serving score goes to a per-user surface.
- The profile tier needs its own calibration curve and its own cohort definition without office type. Reusing the serving calibration, or banding within office type, smuggles the masked columns back in or forces a flat distribution by construction.

Serving shape
- Within-cohort quantile bands, cohort = signup-lead bucket x office type (fall back to lead bucket alone under 100 rows). Absolute values do not transfer: verified-label lead time median 66 days vs live 421 days, distribution overlap 19.5%.
- `expected_corroboration_rate` maps a row's within-cohort percentile to what happened to labelled rows at the same rank and lead bucket (isotonic per bucket), then cuts it into five absolute buckets. Five, not three: a 3-tier cut put 55% in "unsure".
- Scores land append-only: `win_candidate_scores` (campaign_id, model_id, scored_at), `win_candidate_score_context`, and the `win_candidate_scores_latest` view. Re-running a scoring date replaces only that slice.
- Engineered joins and features become dbt mart assets regardless of whether they earned model lift (done for the feature layer in DATA-2315).

## What was tried and did not work (do not re-run without a new reason)

| Idea | Result |
|---|---|
| L2 easy match (exact name + state, zip tie-break) | -0.0020 pooled; lean 5-column variant a wash |
| Full L2/Haystaq block, 791 columns | -0.0046; top-K, PCA and a stacked one-column score all flat. The block's signal is inside the baseline (`L2_BLOCK_SCREEN_METHOD.md`) |
| L2 as a substitute for masked claim columns | Cost AUC in all four masks. Ship no substitution variant |
| HubSpot features | Not built: provenance audit showed they encode the import channel |
| Derived variables, three waves (ratios, cohort-relative values, crowding) | All washes. Don't schedule a fourth wave |
| Race priors from 2024 | Wash pooled, negative forward |
| Name plausibility as features | Negative as features; useful as an intake triage rule |
| CatBoost, char n-grams, isolation forest, TabPFN | Washes or negative. The ceiling is the data, not the learner |
| `election_level` | Deleted: an era marker (100% missing in 2026); `pos_br_level` already carries level |
| A second hand-verification wave | Not worth it; the two-group PU split is already usable |
| Hazard model to admit in-cycle rows for training | Not worth it; only 24 rows would qualify |
| Conformal bands on corroboration | Would guarantee coverage for the wrong target |

What did work: recency weighting; the BallotReady filing-deadline backfill (DATA-2235, +0.0046 from vintage alone); BR position attributes; person-level incumbency at signup; creation-time signup details; opponents-only arrival counts.

## Measured findings worth knowing

- Era drift: 2026 differs from 2025, but no more than 2025 differs within itself. Rolling-origin AUC improves over time, so there's no concept drift; probabilities are biased high in-cycle because of lead time.
- Calibration against corroboration is excellent (pooled ECE 0.006, slope 0.99). Against realness it is off everywhere in the same direction (ECE 0.19). Example: rows with unknown ICP status are calibrated to 0.007 against corroboration and 0.40 against realness.
- Separation is harder inside engaged users: activated 0.80, Pro 0.80, dashboard viewers 0.86, vs 0.87 pooled. Both halves of the ICP split score below pooled, so part of pooled AUC is between-group separation. Always report bootstrap CIs, n and base rate with a subset AUC.
- `is_activated` is identical to "sent at least one campaign".
- Incumbency was tested as a BallotReady coverage proxy and cleared: it discriminates almost equally against BR-sourced (0.561) and non-BR (0.549) corroboration, and composition is additive.
- Scoring at intake is sharp: for signups within 90 days of their election, 66% land in the top bucket (actual 82% corroborated) and 7% in the bottom (actual 1%). For year-ahead signups the model says "not yet" well but rarely "yes".
- Live validation: among already-corroborated live rows, rates rise 22x across serving buckets and 77x across profile buckets. Pro share and ICP share (never model inputs) also rise monotonically across profile buckets.
- Profile-tier read: high-confidence signups skew local (city council, school board, mayor); low-confidence skew congressional. Smaller districts rank higher.
- The party field mixes two spellings from different signup flows (e.g. `Independent` vs `independent`, very different rates). Normalise casing and mask party before quoting any party cut.
- Train/serve skew: `br_arrivals_missing` is 41% in training vs 78% at scoring, because BallotReady has no rosters for future elections yet.
- Phone collection dropped in the 2026 signup flow (99.6% to 72%), so phone-derived features decay at scoring time.

## Gotchas (each one cost time)

- **Dropped exclusion lists.** Re-scores run without `--exclude` re-fired a name-commonness leak and made incumbency look 5x stronger. The tell: a family's standalone AUC jumping with no data change. Fixed by `FAMILY_SPECS`.
- **Label leaks.** `br_candidacy_id` is a label ingredient, so arrival counts must exclude the row's own candidacy. Name-match ambiguity counts leak through the label's entity-resolution name matching; they live in the bridge, never in a feature file.
- **Forward test population changes.** A forward "drop" from 0.8914 to 0.8669 was the test set growing with long-lead rows; on the shared rows the new model was better. Check the population before reading a forward delta.
- **Stale derived artifacts.** `geo_features.parquet` was built from an old population and reached 0.2% of live rows; 1.3% of training rows were silently NaN. Check reach against the target population after any rebuild.
- **Presence-only sources.** L2 vote history and consumer flags: matched-and-null is a real 0. NaN-ing them deletes the negative class.
- **Lead-time labels.** `signup_lead_*` is signup-to-election lead time, not time remaining; `days_to_election` is the other one. A bucket naming a duration must say which.
- **Banding by a claim variable** makes every value of it hold exactly 20% of each band.
- **Positional mapping to bands** silently shifts the whole column when one band is empty. Look up by band edge.
- **Model ids churn.** `model_id` hashes whole source files, so an unrelated edit to `mvp_model.py` mints a new id. Read `model_id` and `mlflow_run_id` off the scored row; never trust an id written in prose. Better fix: hash only the definition structures.
- **Profile run mislabelled.** Until 2026-10-19 `score_population.log_model` hardcoded the serving tier, so profile run `38d903714a5c...` (and UC `win_legit_candidate_profile` v1) carries `tier=serving` and the serving AUCs. Params and metric history can't be edited; the run is re-tagged (`tier`, `area`, `eval_*`, `params_metrics_invalid`). Read profile metrics from the tags, not the metrics panel.
- **MLflow logging.** `log_model` needs `skops` and still fails on HistGradientBoosting; use `serialization_format="cloudpickle"`. Logging raises unless `--allow-unlogged`, because a silent failure once wrote a table with null run ids. Registering in Unity Catalog needs `mlflow-skinny[databricks]` (boto3).
- **`ws_version` covers only the working set.** The serving manifest also hashes geo and the family parquets (`feature_artifact_sha`); consider back-porting to `harness.model_manifest`.
- **Reading mid-append.** Querying the score view while a re-run is between delete and insert returns a partial result that looks like a broken join.
- **Auth.** Use OAuth via `analytics/lib/databricks_conn.get_connection()`, not a PAT from the environment (the PAT path breaks whenever the password manager is locked). `load_web_verify_snapshot.py` still uses the old path.
- **Join keys.** `users_win_candidacy.hubspot_id` is a HubSpot company id; contact-level joins use `mart_civics.candidacy.hubspot_contact_id`. `product_campaign_id` is not unique in `mart_civics.candidacy`; key on campaign plus election date.
- **Haystaq tables are named `l2_*_haystaq_*`**, so name-based bucketing that tests `l2_` first hides them.
- **Model scripts need extras**: the analytics env has no scikit-learn; see each docstring for its `uv run --with ...` line.
- **`build_notebook.py`** does not emit the `eda_summary` cell that was added to the MVP notebook by hand.

## Open questions and next work

- DATA-2565: recurring rescoring job, in-cycle base-rate correction (expected final = observed / maturity at days-to-election), Ops mart column, and an agreement check against the manual verification status that reaches the warehouse through HubSpot. One model rescored on a schedule is the default, not separate models per checkpoint.
- DATA-2566: deliver scores to HubSpot through `reverse-etl/` (config, not code), into their own fields. Gated on a decision about per-lead sales use: the composition bias toward BallotReady-covered geographies was accepted for aggregate analysis only. Scores are campaign grain, HubSpot is contact grain.
- DATA-2567: result-collection tail triage with the score as a prior.
- DATA-2288: Feature Store point-in-time features.
- Re-measure the pass-B baseline under the refreshed geo at the next re-baseline.
- Two L2 block columns above the proxy threshold (via missingness) were kept; drop them or document why.
- Parked: an account-provenance (created-by) audit, which could cleanly split the entry-channel latent; cleaning user-entered election dates against the race calendar (off-date entries corroborate near 0%).
- Drift monitoring should watch four signals: fill rate of columns known to die, adversarial separation between training window and current month, base rate by lead bucket as elections complete, and predicted vs observed per maturing cohort.
