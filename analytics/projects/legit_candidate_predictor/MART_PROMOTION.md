# Feature-layer mart promotion: design note

Drafted 2026-08-13 (Tristan's direction: all variable-creation work lands in dbt feeding the
analytics mart so nobody redoes it). Ticket: DATA-2315, blocked by the composition task
(86ak0je29), which defines the surviving feature list. Extends the 2026-08-07 "engineered
connections are mart assets" decision from joins to the full feature layer.

## What ships

Only what survives composition — the mart carries settled definitions, not every experiment.

| Asset | dbt home | Grain | Content |
|---|---|---|---|
| Signup/product features | `int__win_signup_features` | user | gp_user_details 12-col set (email shapes, phone/area-code, self-report effort) |
| Incumbency | `int__win_candidate_incumbency` | user | office-holder match, PIT-gated to terms starting at/before signup, **now read off `mart_civics.elected_official_terms`**; the governed flag rides alongside it rather than replacing it (see Shipped) |
| BR position attributes | `int__win_position_features` | br_position_id | the accepted 32-col family (sentinel discipline preserved) |
| L2 person bridge | `int__l2_person_bridge` | user | user_id -> LALVOTERID + match_type + confidence/ambiguity diagnostics. Coordinate with matcha DATA-2244 — if the voter_person entity ships, this model consumes it instead of the project-local match |
| Feature mart | `mart_analytics.win_candidate_features` | user | one row per user: accepted features + `as_of` + lineage columns |
| Scores | `model_predictions.*` | user | `p_corroborated` / bands (naming decided 2026-08-05: corroboration language, never "legitimacy"); separate from the feature mart |

Not shipped: experiment-only families (L2 attribute set, race priors, wave-1 ratios), rule-tier
assets (name-plausibility flags belong to intake triage, a different consumer), anything failing
the provenance audits (HubSpot first-touch, entry-channel latents).

## Composition outcome (2026-08-14, 86ak0je29)

All three families survived composition, so the table above ships intact. The authoritative
column list is now `mvp_model.FAMILY_SPECS` (per family: file, key, and a column -> reason
exclusion map), not a remembered command line. Promote from the registry, not from the parquet
column list: the registry's `exclude` entries are decisions, and a column that is in the parquet
but not in the registry is deliberately out.

Serving composition: BR position 32 + gp_user_details 12 + incumbency 7 = 75-column matrix,
0.8824 pooled / 0.8369 decidable / 0.8924 forward (manifest `8480d84d5fab`).

## Tier metadata (third `meta` key, alongside availability)

The analysis tier is a mask over the same columns, not a second feature set, so the mart carries
one column set plus a per-column reserve flag. Add to each promoted column's `meta`:
`claim_reserved_for` (the claim whose analysis-model variant masks this column, or absent),
taken from `mvp_model.CLAIM_MASKS`. Four claims are defined: `incumbency`, `office_level`,
`district_size`, `selfreport_effort`.

Consumer contract: only the serving score reaches a per-user surface. Any analysis that makes a
claim about one of those four concepts must read the matching analysis-tier score, whose
`model_id` is in `manifests/`. The mask spans baseline and family columns alike, which is why
this is column `meta` rather than a family-level attribute.

Measured cost of each tier against the serving model, so consumers can judge whether the
restricted score is good enough for their use: incumbency -0.0041 pooled, office_level -0.0018,
district_size -0.0011, selfreport_effort -0.0029. All four still beat the pass-B baseline.

No L2 substitution variant ships. It was measured on all four masks and cost pooled and
decidable AUC every time (2026-08-14), so the analysis tier is simply the serving feature set
minus the claim's columns. `int__l2_person_bridge` still ships, for record correctness and
analysis-tier joins, which was always its real justification.

## Availability metadata (the "when does this exist" contract)

The PIT exercise this project did by hand must not be redone by the next time-based modeler.
Two mechanisms, neither of which is MLflow (MLflow = model provenance, settled 2026-08-07):

1. **Now, in dbt**: every promoted column carries `meta` in the model YAML —
   `pit_class` (B / A / L), `available_from` (the event that makes it knowable: signup,
   campaign creation, term start, era onset date), `gate` (the rule, e.g. "terms with
   start_at <= signup only", "NaN when question absent from flow"), `source_audit` (pointer to
   the intake doc). Transferred mechanically from `intake/*.md` at promotion time.
   Machine-readable via the dbt manifest; surfaces as catalog comments in Databricks.
2. **Later, Feature Store** (ticket 86ajzyqtn, DATA-2248 Phase 2): feature tables keyed
   (entity, timestamp) with point-in-time training joins. The dbt `meta` scheme above is the
   bridge until its start triggers fire, and the `as_of` column ships now so the tables are
   Feature-Store-shaped on day one.

## Process

- dbt models in `dbt/project` via normal git PR + delegate review (this work exits the
  local-only project dir).
- Dev schema first, promote to marts after validation (house rule).
- Per-column tests: unique/not_null on keys; accepted-value ranges where cheap.
- The project's parquet side tables remain the experimentation layer; the mart is the
  consumption layer. Ledger/baseline workflows keep reading parquets until cutover.

## Sequencing

DATA-2235 rebuild (DATA-2312) -> composition (86ak0je29, surviving feature list + tier masks)
-> this promotion (DATA-2315). The L2 bridge row can move earlier if DATA-2244 wants it.

## Shipped (2026-09-21)

Five models on branch `data-2315/promote-predictor-feature-layer`, plus the
`nanp_area_code_states` seed.

| Asset | Where | Rows | Grain |
|---|---|---|---|
| `int__win_signup_features` | `models/intermediate/win/` | 61,507 | user |
| `int__win_position_features` | `models/intermediate/win/` | 25,656 | br_position_id |
| `int__win_candidate_incumbency` | `models/intermediate/win/` | 43,499 | user |
| `int__l2_person_bridge` | `models/intermediate/win/` | 22,012 | user |
| `win_candidate_features` | `mart_analytics` | 61,507 | user |

The mart carries **all 75 serving columns** (24 baseline + 51 family), verified
against `CAT_COLS + NUM_COLS + FAMILY_SPECS` programmatically, plus `as_of`,
`snapshot_at` and four key/lineage columns. Every column named in all six
`CLAIM_MASKS` entries is present, so every analysis tier can be assembled by
masking the mart rather than rebuilding a feature set.

### Decisions this promotion made, and why

**Incumbency consumes the governed layer without losing the family.** DATA-2491
shipped `int__civics_campaign_incumbency` on 2026-09-18, which the note above
anticipated as "replace with the governed flag". It turned out to be a
*different question*, not a newer answer to the same one: it asks whether a
signup holds the seat they are running for **on election day** (21.3% true,
90.2% known), where the feature family asks whether they held **any** office
**at signup** (16.4%). The first is not knowable at signup, so it cannot enter a
signup-anchored model at all.

The resolution: the seven PIT columns are rebuilt on
`mart_civics.elected_official_terms` instead of the raw S3 staging, and the
governed flag is carried beside them as `is_incumbent_at_election`, labelled
non-PIT. Measured before committing to it: the signup-gated name+state match
gives **16.398% on the governed mart and 16.398% on the raw staging** — bit for
bit the same population — so moving onto the governed mart costs no fidelity.

**The governed flag is NOT on the feature mart.** It is on
`int__win_candidate_incumbency` and already on `users_win_candidacy`. A feature
mart whose contract is "every column is safe for a point-in-time join against
`as_of`" cannot carry a column that breaks it, and the flag is reachable in two
other places.

**`as_of` = signup date; `snapshot_at` = build time.** `as_of` is the anchor the
gates are written against, which is what a Feature Store PIT join needs.
`snapshot_at` keeps the current-snapshot caveat on the BR position block visible
in the data rather than only in prose.

**No label.** `corroborated` stays a modeling construct. The mart is features.

**zip -> state comes from `int__zip_code_to_l2_district`**, not the `zipcodes`
Python package and not the coarse `states_zip_code_range` seed. It resolves a
zip through where L2 voters in it actually live. Cost of the swap, measured:
`zip_campaign_state_match` differs on 346 of 40,067 users (0.86%), mean 0.9878
vs 0.9871.

### Fidelity receipts

Every block was compared column by column against its parquet, on the
overlapping keys, after rendering the Jinja and running against prod:

| Block | Columns | Worst mismatch | Reading |
|---|---|---|---|
| BR position | 32 | `pos_holder_count` 2.7% | all parsed columns (salary, signature count, text lengths, fees) exactly 0 |
| gp_user_details | 12 | `det_n_selfreport_keys` 0.15% | means identical to 4 dp |
| incumbency | 7 | `inc_days_to_term_end` 0.13% | 16.56% -> 0.07% once years-in-office was cast off Spark `DECIMAL` |
| geo | 5 | `zip_campaign_state_match` 0.86% | the zip-source swap above |
| baseline engineered | 13 | `days_signup_to_election` 0.08% | name shapes and `email_domain_class` exactly 0 |

Residuals are source drift between the August parquets and today, plus the
0.26% of users with more than one campaign, where this layer standardises on
one pick (latest election date, then highest campaign id) where the build
scripts each used their own.

### Impossible election dates are discarded (decided 2026-09-21)

Product election dates are user-entered and some are impossible (years `0024`
and `0440`, `4444-04-04`, and `+275760-07-08`, the JavaScript max-date
overflow). The training working set never saw them because it filtered to
2025-2050, but the mart covers every user, so unchecked they arrive as
`days_signup_to_election` up to 99,979,782 — enough to wreck any scaler reading
the mart, which is what a live scoring pass does.

`int__win_signup_features` discards an election date more than **two years
before or twenty years after** the signup, so those rows read as an unknown
election date (a state the column already has, and one an imputer handles)
rather than as a real and absurd distance.

Bound picked off the distribution: p99.9 of the day count is 1,497, the next
values up are 5,419 then 883,873. Nulls **23 of 43,945** users with a date. The
modeled population is bit-for-bit unchanged — the baseline comparison was re-run
after the guard and every column moved by zero. Deliberately kept: five rows
whose election is one to two years *before* signup, which are real elections a
user mis-selected rather than corruption. A range test on both the intermediate
and the mart, bounded to the guard's own window, fails if the guard is removed.

The product-side date validation still wants its own ticket; this stops the bad
values reaching a feature table but does not fix their source.

### Still open

- **137 declared tests pass, 0 failures**, executed against prod the same way.
- The dbt Cloud **dev credential** 403s (`Invalid access token`), so no dev
  build ran. Validation is rendered-Jinja-against-prod throughout, and the
  declared tests were executed the same way. A real `dbt build` is still owed
  before merge.
- L2 bridge cost: the join is against ~218M rows and runs with every build.
  Tagging it `monthly` with the other L2 models was considered and declined
  (2026-09-21): a new signup would go unbridged for up to a month, and the
  signup side is the daily one.


## Review outcome (PR #1045, approved 2026-09-21)

Four rounds, 10 findings: 6 fixed, 4 declined with measurements.

**Fixed.** (1) The mart re-derived `gp_candidacy_id` with a campaign-only join
while the intermediate keyed it on campaign + election date;
`product_campaign_id` is NOT unique in the candidacy mart (87 campaigns, 43
users resolving differently). Fixed by publishing the id from the intermediate
and deleting the mart's copy, so the two cannot diverge again. (2) + (3) A null
email fell through to `'custom'` and made `usr_email_is_role_address` null,
because `null = ''` is null and `split(null,'@')` is null; 0 rows today.
(4) `pos_salary_period` matched period words anywhere in the text. (5)
`pos_seats` had different floors on the two layers. (6)
`pos_election_frequency_years` used `array_max(max(arr))`, which ranks arrays
lexicographically — identical on all 25,620 declared positions, so a latent
trap rather than a live bug.

**Declined, each with numbers.**
- *Null `n_br_by_deadline` for zero-arrival races is corruption.* No: the
  observable zeros ARE 0 (22,457 users). Only the 8,343 with no BR roster at
  all are null, and that is a coverage gap, not an empty field.
- *`inc_is_officeholder_at_signup` should exclude ended terms.* No: that
  collapses it into `inc_serving_at_signup`. The 862 users in the gap are
  former officeholders, a distinct signal. Descriptions extended instead.
- *Drop the null guards from the incumbency spine.* No: all 18,008 excluded
  users are missing `campaign_state`, so there is no key to match them on;
  emitting 0 would assert "not an officeholder" for people we cannot check.
- *`max(databaseid)` may pick a filing period with a null `end_on`.* No: across
  1,111,804 races, the number where the chosen period is null while another is
  usable is **0**.

**The word-boundary lesson.** The reviewer's suggested fix for the salary
period was a full `\bword\b`. Measured across all 2,007 distinct salary
strings, that breaks 28 positions to fix 2 — it discards `$750/monthly`,
`board meetings` and `bi-weekly`. A LEADING boundary only, `(^|[^a-z])word`,
fixes the single real false positive (`determined` -> term) and changes nothing
else. Measure the suggested fix, not just the reported bug.
