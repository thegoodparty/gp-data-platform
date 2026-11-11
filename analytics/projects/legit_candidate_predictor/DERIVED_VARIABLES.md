# Derived-variable seed list (stage-7 kickoff)

Drafted 2026-08-12 for Tristan to react to. Scope settled in SESSION_NOTES: only the three
classes a tree cannot learn on its own. Monotone reshaping of single variables (quantile bins,
logs) is explicitly out — free to HGB. Construction rules: label-free (the one exception is
fold-internal by design), trailing windows for any cohort statistic, PIT on the signup clock.

Status legend: READY (buildable now) / POST-REBUILD (cheaper or only correct after the
DATA-2235 working-set rebuild) / TESTED (already measured, listed for completeness) /
AREA-1 (label-adjacent — belongs to label work, not features).

## Class 1 — true ratios (numerator and denominator both exist; the quotient is the concept)

| # | Variable | Formula | Pools | Status | Why |
|---|---|---|---|---|---|
| 1.1 | `signature_burden` | pos_signature_count / voter_count | BR + L2 district | READY | The 0.968 correlation flagged at intake is the point: the RATIO is the residual — petition burden relative to district size. Extracts what the raw count can't (it's just district size). |
| 1.2 | `fee_per_voter` | pos_max_filing_fee / voter_count | BR + L2 district | READY | Raw fee failed (importance ~0). Cost-of-entry was always a relative concept; a $500 fee is a filter in a village and noise in a city. |
| 1.3 | `crowding_per_seat` | n_br_candidacies_by_deadline / pos_seats | BR | TESTED — wash | Built 2026-08-13 post-rebuild on opponents-only arrivals. Highest importance of the three (0.020 forward) but the family delta is noise. Zero-inflated by construction: 90% of non-null values are 0, because most BR rosters arrive after the filing deadline. |
| 1.4 | `signup_lateness_fraction` | (signup - filing_window_start) / window length | BR + product | TESTED — wash | Built 2026-08-13. 61.2% fill, capped by BR window-start coverage (62.3%, against 79.4% for the deadline). Left unclipped on purpose; see the intake cleanup note. |
| 1.5 | `vote_completion_rate` | l2_n_generals_voted / generals eligible since registration | L2 | READY, low hope | Tristan's quartile example done right (eligibility-adjusted). Honest caution: L2's own precomputed version (propensity) was already absorbed; test cheap, expect wash. |
| 1.6 | `effort_density` | det_n_selfreport_keys / keys offered in that flow era | product | READY | Effort relative to what the flow asked. Converts the era-drifting count into an era-stable share. Overlaps 3.1 — build one, not both. |

## Class 2 — candidate assertion vs external record (reframed 2026-08-13, Tristan's challenge)

The class is only about the candidate when one side is something the candidate TYPED OR CHOSE
and the other is an independent record of reality (registration, voter file, race calendar).
When both sides are pipelines (product scaffolding vs BR, HubSpot vs product), agreement measures
our own data synchronization — the candidate has no control over it — and the item is
data-quality/label-hygiene work, not a feature. 2.1 and 2.5 are demoted on exactly this ground;
2.2/2.3/2.4 survive the test as candidate-assertion checks (2.4 still stays Area-1 because the
mismatch also breaks the label join, which makes it circular for us regardless of what it says
about the candidate). Caveat that stays with the class: an assertion-vs-record mismatch can also
mean WE matched the wrong record, so each variable needs its match-error channel separated
before the candidate reading is trusted.

| # | Variable | Formula | Pools | Status | Why |
|---|---|---|---|---|---|
| 2.1 | `deadline_agreement` | product filingPeriodsEnd vs BR deadline | product + BR | DEMOTED to data-quality | Both sides are pipelines; candidate controls neither. Keep as the post-rebuild reconciliation DIAGNOSTIC only. |
| 2.2 | `zip_disagrees_on_unique_match` | user_zip vs L2 residence zip, unique matches only | product + L2 | READY | For exact-unique matches, zip disagreement = recent move or wrong-person match. Match-precision diagnostic first, weak feature second. |
| 2.3 | `party_matches_registration` | declared party vs L2 Parties_Description | product + L2 | TESTED | Built in the L2 family; only 20% comparable, absorbed. Keep in the pool for analysis variants. |
| 2.4 | `election_date_on_calendar` | user-declared election date vs BR race calendar | product + BR | AREA-1 | Off-date Nov-2025 entries corroborate ~0% — but partly MECHANICAL (the label join is date-matched), so it's label hygiene, not a feature. Clean the dates; don't feed the model its own join key. |
| 2.5 | `office_disagreement` (HubSpot) | product office vs HubSpot company office | product + HubSpot | DEAD | Pipeline-vs-pipeline AND provenance-poisoned: disagreement corroborates HIGHER (81% vs 64%) = enrichment attention. |

## Class 3 — cohort-relative (row vs peer-group statistic; requires cross-row aggregation)

All use trailing windows (prior signups only) so they are PIT-clean at scoring time.

| # | Variable | Formula | Pools | Status | Why |
|---|---|---|---|---|---|
| 3.1 | `effort_vs_era` | det_n_selfreport_keys - trailing-90d signup-cohort median | product | READY | The era-drift treatment for the family's best feature. Alternative to 1.6. |
| 3.2 | `voter_count_vs_office_norm` | voter_count / trailing median for (office_type) | L2 district + BR | READY | Unusually large or small district for this office type — separates "big race" from "big office type". |
| 3.3 | `crowding_vs_race_norm` | 1.3 MINUS trailing median for (state, office_type) | BR | TESTED — wash | Built 2026-08-13. Formula changed from ratio to DIFFERENCE: the trailing median crowding is 0 almost everywhere, so the ratio was defined on 1.1% of rows. Same rule wave 1 used (subtract for count-like with mass at zero, divide for strictly positive magnitudes). Lowest importance of the three, 0.0004. |
| 3.4 | `days_to_election_vs_office_norm` | days_signup_to_election - trailing median for (office_type) | product + BR | READY | Early-for-this-office signal; lead time is the label's biggest structural driver (era-drift finding), so normalising it by office norms may separate behavior from calendar mechanics. |
| 3.5 | `race_prior_corroboration_rate` | historical corroboration by (position type / state), cycles strictly before the row's election year, computed INSIDE CV folds | label + BR | READY, needs harness support | Catalog item 5. The one deliberately label-using derived variable; both guards (prior cycles only + fold-internal) are mandatory or it's target leakage that looks great and fails forward. `exp_family.py` cannot compute per-fold features today — small extension required. |

## Sequencing proposal

1. **Wave 1 (now):** 1.1, 1.2, 3.2, 3.4 — all from columns already in accepted families, one
   build script, one experiment vs the 0.8683 baseline.
2. **Wave 2 (needs harness extension):** 3.5 race priors — extend exp_family for fold-internal
   feature computation first.
3. **Wave 3: DONE 2026-08-13 (DATA-2312), WASH.** 1.3, 1.4, 3.3 built on the rebuilt working
   set (ws `b04cdd5ac4b1`): +0.0005 pooled / +0.0008 decidable / +0.0039 forward, family alone
   0.7525. 2.1 not built; it needs a details-blob join and stays a reconciliation diagnostic.

   **All three waves are now washes.** The wave 1 read holds and is now the general finding:
   the baseline already carries these relationships, and derivation pays only where the
   quotient or residual is genuinely absent from the feature set rather than merely implicit.
   A fourth wave should not be scheduled without a specific reason to expect otherwise.
4. On demand: 1.5/1.6/3.1 (cheap, low expectation), 2.2 (as matcha QA input).

## Standing cautions

- A ratio inherits the leakage class of its worst ingredient (crowding uses by_deadline —
  sentinel discipline applies; anything touching arrivals carries the coverage-proxy caveat).
- Cohort medians are label-free population statistics, but race priors (3.5) are not — fold
  discipline is what separates them from target encoding.
- Every wave is one ledgered experiment against the canonical baseline; accepted items get the
  same intake treatment as a family (the cross-family covariance check now exists to catch
  ratio-vs-ingredient redundancy automatically).
