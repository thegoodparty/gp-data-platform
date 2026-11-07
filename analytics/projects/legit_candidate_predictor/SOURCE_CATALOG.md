# DATA-2239 Area 2: warehouse source catalog and feature opportunities

Built 2026-08-06 from `information_schema` plus live coverage probes. Everything below with a
number attached was measured against the predictor population, not inferred from column names.

Rerun the inventory: `uv run --with pyarrow python projects/legit_candidate_predictor/catalog_inventory.py --force`
Rerun a coverage probe: `uv run --with pyarrow python projects/legit_candidate_predictor/probe_join_coverage.py <probe>`
Caches: `datasets/catalog_columns.parquet` (170,829 column rows), `datasets/catalog_tables.parquet` (1,122 tables).

Population for every coverage number below = `build_working_set.py`'s population re-derived in SQL
(latest version, not demo, completed generals 2025-01-01 onward): **20,478 rows** (the frozen working
set has 20,480; the warehouse has drifted by 2, same as the 0b audit found).

L2 and voter data are handled aggregate-only here. No individual-level records were extracted.

## 1. What is in the warehouse

`goodparty_data_catalog` only (plus `hive_metastore`, `samples`, `system`, which hold nothing for us).
1,379 tables across 14 schemas. Feature-relevant schemas and their families:

| Family | Tables | Where | What it is | In the model today |
|---|---|---|---|---|
| ballotready | 44 | airbyte_source, dbt | races, positions, candidacies_v3, office_holders_v3, filing periods, recruitment_v1, us cities/counties | yes: race, election, candidacies_v3, filing_period |
| gp_api_product | 84 | airbyte_source, dbt | product DB: campaign, user, position, outreach, poll, website, ecanvasser, p2v, tcr | partly: via mart_analytics.users_win_candidacy |
| l2 | 332 | dbt (51 states x 6 files), airbyte_source, model_predictions | nationwide uniform voter file (807 cols, **218.1M rows**), vote history, district aggregations, LLM L2-to-BR district match | only indirectly: voter_count from int__icp_offices |
| haystaq | 108 | dbt | modeled person-level opinion and behavior scores keyed on LALVOTERID: `int__l2_nationwide_haystaq_scores` (406 cols), `_flags` (187 cols), `int__l2_nationwide_uniform_w_haystaq` (1,396 cols), 51 state score + 51 state flag tables, `haystaq_issue_tags`. **212.6M rows, 97.5% of the voter file, all 51 states** | no |
| segment | 172 | dbt | per-event web_app and gp_api tables, plus pages/tracks/identifies | no |
| amplitude | 24 | airbyte_source, dbt, mart_analytics | raw events (59 cols), event catalog, win/serve activity, user milestones | yes: geo features, pass-A activity |
| hubspot | 35 | airbyte_source, dbt, mart_analytics | **companies** (candidacy grain, carries candidate fields), **contacts** (person grain, 353 cols staged, 893 in SCD2 snapshot), deals, tickets, engagements, calls | no |
| ddhq | 33 | airbyte_source, dbt, model_predictions | election results, races, embeddings, dated matcher outputs | yes: label only, via candidacy_stage |
| techspeed | 14 | airbyte_source, dbt | vendor candidate and officeholder files, marketing enrichment | yes: label only |
| civics_entity_resolution | 19 | dbt | person/candidacy clustering, canonical ids, estimated opponents, viability scoring | yes: label via candidacy_stage |
| census | 4 | dbt, mart_civics | ACS geo estimates (86 cols), block population, district census stats | no |
| stripe | 72 | airbyte_source, dbt, dbt_stripe | subscriptions and payments | no (post-signup by nature) |
| icp | 2 | dbt | int__icp_offices, normalized position names | yes: icp_office_win, voter_count |
| model_predictions | 24 | model_predictions | turnout, ballots projected, viability WOE, dated BR/DDHQ matcher runs | no (viability parked pending PIT) |
| other election_api | ~25 | dbt | enhanced place/position/race, fast facts, zip-to-position, people_api voter (355 cols) | no |

Not feature-relevant: clickup, github, exports_zapier, mban, sandbox, private_* schemas.

## 2. Verified join opportunities, ranked by measured coverage

Every row is a probe result, with the probe name in the last column.

| Source | Join key | Coverage of 20,478 | Verdict | Probe |
|---|---|---|---|---|
| gp_api user table | `user.id = user_id` | 100% (phone 99.6%, avatar 5.9%, has_password 42.7%, meta_data 100%) | build it, cheap | `user_table`, `user_meta` |
| BR position attributes | `position.database_id = br_position_id` | 88.0% (18,018; = the decidable population) | build it, best untapped race context | `race_priors` |
| HubSpot **companies** (candidacy grain) | `hubspot_id = companies.id` | **96.6%** (19,791 users) | build it, this is what `hubspot_id` actually is | ad hoc, section 3 |
| HubSpot **contacts** (person grain) | `candidacy.hubspot_contact_id` | **99.9%** (20,458 users) | build it, but only the fields nothing else already has | `hubspot_keys`, `hubspot_fields_email` |
| Name plausibility from voter-file name frequency | none, name text only | 100%, and it needs no person match | build it, cheapest new axis | `name_plausibility`, `name_patterns` |
| L2 voter file, own record | (state, first, last) exact | 78.6% any match, 41.2% unique, 37.4% ambiguous | build it, top of ticket confirmed | `l2_name` |
| Haystaq person scores | `LALVOTERID`, gated on the L2 match | 97.5% of voter records, so about 76% of the population once matched | build it with the L2 match, no extra matching cost | ad hoc, section 4.4 |
| Sales/CRM provenance (`prospects`) | `gp_user_id` | 92.8% in table, 77.1% have a sales touch, 37.2% touched before signup | build it, but leakage audit first | `prospects` |
| BR office holders | `br_position_id` | 80.0% of rows have a holder record | build it with the incumbency work | `race_priors` |
| Campaign `details` blob keys | `campaign.id` | see key inventory below, 3% to 92% by key | build the creation-time keys | `details_keys` |
| Amplitude **pre-signup** via device_id | device_id bridge | **3.3%** (686 users) have any pre-signup event | drop this one narrow use | `amplitude_device` |
| Amplitude **post-signup** product behavior | `user_id` | 526 event types, 322 Win, 44% of post-2025-05 signups have browser events | build the family-level scaffold, pass A | `download_archetype`, section 4.8 |
| Segment web behavior | n/a | 2.6% in identifies; and it reaches us through Amplitude anyway | drop, duplicative | `segment` |
| Airbyte campaign history for as-of-signup | `_airbyte_extracted_at` | history starts **2026-04-13**; only 22 rows have an extract within 7d of signup | as-designed reconstruction is impossible | `campaign_history`, `campaign_gen` |
| HubSpot SCD2 snapshot | `candidacy.hubspot_contact_id` | untested; the 0% result was the wrong key | recheck if as-of-signup HubSpot values are ever needed | `hubspot_snapshot` |

Everything in this table is measured. The `mart_civics.people` (89.3%) and `candidate` (89.6%) bridges
also work but are strictly worse than the `candidacy` one, and they carry `br_person_id`, so any feature
drawn through them needs a leakage review first.

## 3. Findings that change the ticket

**Item 2's narrow claim is dead: there is no pre-signup session behavior to mine.** 3.3% of the
population has any pre-signup event on a device it later used. Segment's identifies table starts
2026-02-01 and reaches 2.6%. HubSpot's `hs_analytics_first_visit_timestamp` exists for 99.8% of joined
rows but sits on the signup date itself for 82% of them (median first-touch lag 0 days, 8% strictly
pre-signup). Three independent sources agree: our population mostly arrives and converts in one
session. What does exist is first-touch *attribution*, a different feature family, nearly fully covered.

**This is not a verdict on Amplitude.** Scope the finding to the pre-signup device merge only.
Post-signup product behavior is a separate, rich, and deliberately-built family, covered in 4.8. It is
pass A rather than pass B, so it cannot go in the analysis filter, but it is where archetype questions
get answered and it is where forward coverage improves fastest.

**Item 3 (details blob as-of-signup reconstruction) has no history to reconstruct from, and mostly
does not need one.** The Airbyte raw campaign table holds a single generation starting 2026-04-13
(106,463 rows over 62,998 campaigns), so as-of-signup values cannot be recovered for historical rows.
But the key inventory shows the high-coverage keys are written once at campaign creation (race
scaffolding), so their current value already is the as-of-signup value. The keys that genuinely mutate
are the Pro and subscription and outcome keys, which are excluded on leakage grounds anyway. That
turns the long pole into a short one, plus one validation: use the 2026-04-13 onward CDC window to
measure which keys actually change between versions, and treat any key that mutates as not-PIT.

**Item 1 (L2 match) is confirmed and bigger than expected.** 78.6% of the population matches a voter
record on exact (state, first name, last name); 41.2% match exactly one. Section 5 covers how to move
the 37.4% ambiguous bucket.

**HubSpot has two grains, the mart already carries both ids, and no email fallback is needed.**
Corrected twice on 2026-08-06: first draft wrongly called the join broken, second draft wrongly
recommended email. `users_win_candidacy.hubspot_id` is the **company** id (96.6% of users match
`hubspot_api_companies.id`; 56,713 of 58,649 ids overall). It matches contacts zero times, which is
correct rather than a bug: in this portal a company is the candidacy and a contact is the person.

`mart_civics.candidacy` carries both, at essentially full coverage, measured on the population:

| Bridge | Grain | Coverage of 20,477 users |
|---|---|---|
| `candidacy.hubspot_contact_id` | person | **20,458 (99.9%)** |
| `candidacy.hubspot_company_ids` | candidacy | **20,462 (99.9%)** |
| `candidate.hubspot_contact_id` via `prod_db_user_id` | person | 18,350 (89.6%) |
| `people.hs_contact_id` via `gp_api_user_id` | person | 18,293 (89.3%) |
| lowercased email | person | 20,396 (99.6%) |
| `contacts.goodparty_user_id` | person | 5,673 (27.7%) |

Use `mart_civics.candidacy` for both grains. Drop the email fallback and drop the proposed
`goodparty_user_id` backfill ticket, neither is needed.

**EIN was collected, contrary to the parked assumption.** The parking lot says "EIN cannot be
backtested (never collected)". The campaign details blob has `einNumber` on 695 campaigns (590
non-empty), `validatedEin` on 671, and `einSupportingDocument` on 696. It arrives through the Pro
upgrade committee check, so it is Pro-gated and post-signup, which means it is not a pass-B feature.
It is still a near-bulletproof label sharpener for the ~3% who have it, and the team's "we never
collected EIN" premise should be corrected.

## 4. Feature deep dive by source

Notation: **B** = pass-B legal (knowable at or before signup, immutable after), **A** = pass-A only
(post-signup or mutable), **L** = leakage risk that needs an explicit audit before use.

### 4.1 BR position attributes (88% coverage, entirely new, cheapest high-value build)

`stg_airbyte_source__ballotready_api_position` joined on `br_position_id`. These are static race
properties, not candidate behavior, so they are B by construction.

| Feature | Definition | Coverage | Why it should discriminate |
|---|---|---|---|
| `pos_max_filing_fee` | `maximum_filing_fee` | 46.7% | A filing fee is a real cost of entry. Noise signups do not pay fees. |
| `pos_filing_requirements_len` | character length of `filing_requirements` | 85.8% | Proxy for procedural burden. Harder ballots filter harder. |
| `pos_requires_signatures` | regex for "signature" in `filing_requirements` | 85.8% | Petition requirements are the strongest known entry filter. |
| `pos_min_age`, `pos_must_be_registered`, `pos_must_be_resident` | as named | 76% / 83% / 83% | Eligibility gates; also interact with the L2 match. |
| `pos_seats`, `pos_seats_per_voter` | `seats`, `seats / voter_count` | 88% | Multi-seat races have lower entry bars and different noise rates. |
| `pos_salary_present`, `pos_salary_amount` | parsed `salary` | 17.1% | Paid seats attract different candidates than unpaid ones. |
| `pos_election_frequency_years` | parsed `election_frequencies` | 88% | Off-cycle and infrequent seats behave differently. |
| `pos_has_primary`, `pos_is_partisan`, `pos_is_judicial`, `pos_is_appointed`, `pos_tier` | as named | 88% | `is_appointed` in particular is near-tautological for "will never appear on a ballot". |
| `pos_holder_count`, `pos_holder_is_vacant` | from office_holders_v3 | 80% | Vacancy and incumbency shape the field. |

This family is the best expected-value-per-hour build on the list: one join, no matching, 88%
coverage, and it is orthogonal to everything already in the model (which sees only
`br_normalized_position_type`, `office_type`, `election_level`, `voter_count`).

### 4.2 HubSpot companies, candidacy grain (96.6% coverage)

`stg_airbyte_source__hubspot_api_companies` on `hubspot_id`. 38 columns, and it is a sales-maintained
mirror of the candidacy, so treat every field as mutable unless proven otherwise.

Possibly useful, all needing an as-of-date check first:
- `candidate_district` (31.4%), `candidates_seats` (36.1%), `candidate_party` (47.4%),
  `official_office_name` (65.0%), `office_level`, `office_type`, `state`
- derived: does the HubSpot-recorded office agree with the product-recorded office. Disagreement
  between what the user typed and what sales recorded is a plausible noise signal, and it is
  independent of BR.

Leakage, exclude from pass B: `br_race_id` (90.4%) and `verified_candidates` (88.7%). Both are
downstream of the same corroboration pipeline that makes the label.

Net read: lower value than the contact grain. The high-fill fields here duplicate what
`users_win_candidacy` already carries, and the genuinely new fields are the leaky ones. Build it after
the contact grain, and only for the product-versus-sales disagreement feature.

### 4.3 HubSpot contacts, person grain (99.9% via `candidacy.hubspot_contact_id`)

**One concept, one source.** Most of what HubSpot holds we already have from Amplitude or the mart, and
importing it twice buys nothing but correlated columns. The rule for this family is: for each concept,
pick the single highest-coverage source and take it only from there. Measured overlap:

| Concept | HubSpot | Also in | Take it from |
|---|---|---|---|
| First-touch source, channel, referrer | `hs_analytics_source` 100% of joined, `channel_group__first_conversion_source` 99.5%, `hs_analytics_first_referrer` 37% | Amplitude `user_properties`: `initial_referrer` 4,554 users, `initial_utm_source` 3,334, `gclid` 1,346 | **HubSpot** (4x the coverage); use Amplitude `initial_*` only to cross-check |
| UTM campaign, medium, term | `utm_*` under 1% | Amplitude `initial_utm_medium` 3,328, `initial_utm_campaign` 3,318 | **Amplitude** |
| Signup method | not carried | Amplitude `signUpMethod` 2,472; gp_api `has_password` 100% | **gp_api** |
| Office, level, type, state, district, party | present, various fill | `users_win_candidacy`, `mart_civics.candidacy`, BR position | **mart / BR**, skip HubSpot |
| Incumbency, open seat, opponents, population | `is_incumbent` 70%, `population` 51%, `number_of_opponents` 29% | `mart_civics.candidacy.is_incumbent` / `is_open_seat`, `int__civics_election_estimated_opponents`, `int__l2_district_aggregations` | **mart**, skip HubSpot |
| Pledge | `is_pledged`, `pledge_status` | `users_win_candidacy.is_pledged`, details `pledged` 92% | **product** |
| Website and socials | `website`, `facebook_url`, `twitter_handle`, `linkedin_url`, `instagram_handle` | `mart_civics.candidate` carries all five plus `urls` | **mart_civics.candidate** |
| Birth date, age | `birth_date` 6% | L2 `Voters_BirthDate` and `Voters_Age` on 78.6% matched | **L2** |
| Verification status | `verified_candidate_status` 87% | `candidacy.is_verified`, `verification_status_reason` | **neither**, this is label leakage |

So the only thing HubSpot uniquely wins is **first-touch source, channel group, and first referrer**.
That is the whole build for this family. Everything else in the table above comes from somewhere we are
already joining.

Two disjoint groups within what remains. Keep them apart.

Pass-B legal, first-touch attribution (immutable by definition, and pre-signup):
- `hs_analytics_source` (100% of joined), `hs_analytics_source_data_1/2`
- `channel_group__first_conversion_source` (99.5%), `conversion_source` (91.9%)
- `hs_analytics_first_referrer` (37.3% in 2025, 79% pre-2025), `hs_analytics_first_url`
- `hs_analytics_first_touch_converting_campaign`, `utm_*` (thin: `utm_source` under 1%)
- derived: `first_touch_to_signup_days` = signup minus `hs_analytics_first_visit_timestamp` (only 8%
  strictly positive, so encode as a three-way categorical: prior visit / same-day / unknown)
- derived: `is_paid_acquisition`, `is_organic_search`, `is_direct`, `is_referral` from channel group
- derived: `source_x_state` and `source_x_office_level` interactions, since acquisition channel mix
  almost certainly varies by geography and office tier

Not pass-B, and some are label-adjacent (do not feature-ize without an audit):
- `hs_analytics_num_visits`, `num_page_views`, `average_page_views` are cumulative to now: **A**
- `is_filed_candidate`, `is_confirmed_candidate`, `verified_candidate_status` (86.9% filled),
  `candidate_id_source`, `candidate_id_tier`, `candidate_date_verified`: **L**, these are downstream
  of the same verification pipeline that produces the label. They belong to Area 1 label fusion, not
  Area 2 features.
- `br_position_id`, `br_candidacy_id`, `br_race_id` on the contact: **L**, direct label leakage.
- `is_incumbent` (70.5%), `population` (51.4%), `number_of_opponents` (29.1%): enrichment of unknown
  provenance and unknown as-of date. Treat as **L** until we trace where they are written from. If
  they are BR-derived, they are a coverage proxy in disguise, which is the exact failure the sentinel
  fix just removed.
- `birth_date` (6.2%): B where present, gives `age_at_signup`. Also a disambiguator for the L2 match.

### 4.4 L2 voter file (78.6% match, 41.2% unique)

**Measured first: how much is "is this a real registered human" worth?** Corroboration rate by match
bucket, against a 59.4% base rate:

| Match bucket | n | Corroboration rate |
|---|---|---|
| no match in declared state | 4,387 | **51.9%** |
| exactly one match | 8,430 | 61.4% |
| 2 to 5 matches | 4,141 | **63.1%** |
| more than 5 matches | 3,519 | 59.1% |

Two readings, both important. The match itself is worth about 10 points between no-match and matched,
which is real but modest, not the step change the ticket's expected-value ranking implies. And the
ambiguity gradient is non-monotonic: two-to-five matches corroborate *higher* than unique matches. So
disambiguating the ambiguous 37.4% is **not** where label signal lives. Disambiguation still matters,
but for a different reason: attaching the correct person's vote history, donation record, and haystaq
scores requires the right individual. Treat the two as separate jobs with separate justifications, and
do not sell the zip and nickname work as an AUC play.

Once a candidate is matched to their own voter record, the feature space is large. All of it is B if
and only if the snapshot fields used are not themselves consequences of running. Gate carefully.

Self-record features, B:
- `l2_matched` (the match itself, plus `l2_match_ambiguity` = number of same-name records)
- `l2_registered_in_declared_state` (already implied by the state-scoped match; the informative
  version is a nationwide match that reveals the person is registered somewhere else)
- `l2_is_active` (`Voters_Active`, 98.2% of matched), `l2_years_registered` from
  `Voters_CalculatedRegDate` (100% of matched), `l2_party` and `l2_party_matches_declared_party`
- `l2_age`, `l2_age_minus_position_min_age` (interacts with `pos_min_age` above)
- `l2_vote_propensity`: `Voters_VotingPerformanceEvenYearGeneral` and the primary and minor-election
  variants. These are compact pre-computed scores, so no vote-history join is needed.
- `l2_voted_in_prior_cycles`: `AnyElection_*` restricted to years strictly before signup. **The
  `AnyElection_<signup year>` and later columns must be excluded**: voting in your own election year
  is contemporaneous with running, and for a 2025 signup `AnyElection_2025` is post-signup information.
- `l2_household_voters` (`Residence_Families_HHVotersCount`), `l2_moved_recently`
  (`Voters_MovedFrom_Date`), `l2_phone_type` (cell only vs landline) to cross-check the product phone
- `l2_fec_donor`: `FECDonors_NumberOfDonations`, `TotalDonationsAmount`, and critically
  `FECDonors_LastDonationDate` so donations after signup can be excluded. Prior political giving is a
  strong prior on political seriousness.
- `l2_ses`: `ConsumerData_Estimated_Income_Amount`, `Education_of_Person`, `Occupation_of_Person`,
  `Homeowner_Probability_Model`. Use with intent: these carry equity risk if the score ever drives
  sales or marketing decisions rather than analysis filtering, which is already a parked v1 caveat.
- `l2_geo`: `Residence_Addresses_Latitude/Longitude` unblocks the distance feature that Amplitude
  could not support. Distance from residence to the district centroid or the position's place is a
  direct "is this person plausibly in this district" test.

Household and district context, B, from `int__l2_district_aggregations`: `voter_count` (already in the
model), `unique_cellphones`, `unique_landlines`.

### 4.5 Haystaq person scores (97.5% of the voter file, so about 76% of the population once matched)

This family was missing from the first draft of this catalog because the inventory's naming rule
bucketed every `l2_*_haystaq_*` table under L2. Corrected: 108 dbt tables, and it is the largest
single block of person-level signal in the warehouse.

`int__l2_nationwide_haystaq_scores` (406 columns, 212.6M rows) and `int__l2_nationwide_haystaq_flags`
(187 columns) are keyed on `LALVOTERID`, so they attach for free to whatever the L2 person match
resolves. `int__l2_nationwide_uniform_w_haystaq` (1,396 columns) is the pre-joined version.

These are modeled propensity scores, not observed behavior, so they are B: they describe the person as
the vendor models them and are not consequences of running for office. The relevant ones are unusually
on the nose for this problem:
- `hs_activism` — modeled political activism propensity. This is the single most on-target column name
  in the warehouse for "is this person actually a political actor".
- `hs_political_donations_likely` / `_unlikely` — independent of, and cross-checkable against, the
  L2 `FECDonors_*` observed-donation fields.
- `hs_partisanship_moderate_third_party_support` / `_oppose` and `hs_ideology_overall_party_indep` —
  independent and third-party affinity, which is our actual candidate population.
- `hs_campaign_finance_reform_high_priority`, `hs_rank_choice_voting_support` — reform-issue salience.
- `hs_likely_presidential_voter`, `hs_likely_mid_term_voter`, `hs_likely_polling_turnout`,
  `hs_likely_vbm`, `hs_likely_ev` — modeled turnout propensity, complementary to the observed
  `VotingPerformance*` fields.
- `hs_responsiveness_email` / `_sms` / `_live` — survey responsiveness, a proxy for engageability.
- The `hf_*` flag versions of all of the above, which are categorical rather than continuous and so
  cheaper for a tree model to use.

Two cautions. First, dimensionality: 400 correlated modeled scores against 20k rows will invite
overfitting, so go in with a pre-declared shortlist (the dozen above) rather than the full block, and
only widen if the shortlist pays. Second, these are the most equity-sensitive columns in the catalog,
inferred ideology and issue positions attached to named individuals. Fine for an analysis filter,
which is the v1 acceptance; a hard stop for anything that drives per-lead sales or marketing action
without a separate review.

Coverage caveat: 97.5% is haystaq's coverage of the voter file, not of our population. The population
number depends on the L2 match and is therefore about 76% at the current exact-match rate. Measure it
directly when the match lands.

### 4.6 gp_api user and campaign details (100% and per-key)

User table, B unless noted:
- `phone_valid` (99.6% have a phone), `phone_area_code_state_matches_declared_state`,
  `phone_is_voip_or_toll_free` pattern check
- `auth_method`: `has_password` is 42.7%, so 57% arrived through social or magic-link auth. Real
  variance, cheap, and plausibly correlated with intent.
- `email_domain_is_free`, `email_is_role_address` (info@, campaign@), `email_local_part_matches_name`,
  `email_contains_office_words` ("forcitycouncil"), `email_is_goodparty_org`. Some of this exists in
  v1's name and domain features; the office-words and name-match variants do not.
- `avatar_present` (5.9%): **A**, avatars can be uploaded at any time and there is no timestamp.
- `meta_data` keys: `fsUserId` (FullStory instrumented, 2025 only), `sessionCount` and `lastVisited`
  and `profile_updated_count` are all mutable counters, so **A**. `whyBrowsing` and `accountType`
  exist on fewer than 60 rows and are dead.

Campaign `details` keys, with measured non-empty counts across the population (2025H1 / 2025H2 / 2026):

Creation-time, high coverage, B:
- `ballotLevel` 6,858 / 10,711 / 629 — the verified `election_level` fix source from the 0b audit
- `pledged` 7,072 / 11,220 / 600, `hasPrimary`, `partisanType`, `officeTermLength`, `tier`
- `filingPeriodsEnd` 6,050 / 9,812 / 570 (about 80% of the population) — **an independent
  product-side filing deadline captured at onboarding**, worth reconciling against the BR-derived
  deadline that DATA-2235 is fixing
- `priorElectionDates` 6,281 / 8,582 / 115, `subAreaName` and `subAreaValue`, `electionCounty`
- `knowRun` 3,608 / 7,689 / 350 (about 58%) — a self-reported onboarding answer about knowing what
  they are running for. This is the closest thing we have to a stated-intent feature.

Sparse but semantically strong, B:
- `occupation` about 10%, `pastExperience` about 8%, `website` about 8%, `runningAgainst` about 8%
  (self-reported opponents), `campaignCommittee` about 7%, `customIssues`, `funFact`

Excluded:
- `einNumber` / `validatedEin` / `einSupportingDocument` about 3%: **A**, Pro-gated. Label sharpener.
- `isProUpdatedAt`, `subscription*`, `wonGeneral`, `primaryResult`, `afterElectionEmailSent`: **A** or
  outright outcome leakage.
- `ballotStatus` (12 rows) and `hasFiledForRace` (4 rows): the self-reported ballot status the ticket
  hoped for effectively does not exist.

### 4.7 Name plausibility (new, and it does not need the person match)

The model's existing name features are purely syntactic: token count, length, has-digit,
has-non-alpha, single-character-token. Nothing asks whether the name is a name. The voter file answers
that at population scale, and it does so with two small frequency tables rather than a person match,
so this build is independent of the matcha decision entirely.

Measured, against the 59.4% base rate:

| Signal | n | Corroboration rate |
|---|---|---|
| neither first nor last name occurs in the 218M-row voter file | 48 | **4.2%** |
| first name equals last name | 24 | **8.3%** |
| a name contains a tripled character | 11 | **18.2%** |
| first name unknown to the voter file | 219 | 40.6% |
| last name unknown to the voter file | 359 | 37.3% |
| placeholder token (test, demo, asdf, admin, none, fake, xxx) | 36 | 44.4% |
| first or last name 2 characters or fewer | 146 | 45.9% |
| exact first-last pair never seen nationwide | 2,729 | 50.9% |
| both names rare (fewer than 50 occurrences each) | 1,774 | 53.3% |
| both names common | 18,078 | 60.7% |

Proposed features, all B, all cheap:
- `name_first_freq_us`, `name_last_freq_us`, `name_pair_freq_us` as log counts from two precomputed
  dimension tables (`first_name -> count`, `last_name -> count`) plus a pair table. Build them once from
  L2 and cache; no repeat scan of 218M rows.
- `name_first_unknown`, `name_last_unknown`, `name_both_unknown`
- `name_fn_eq_ln`, `name_placeholder_token`, `name_tripled_char`, `name_token_too_short`
- `name_keyboard_run` (asdf, qwer, zxcv adjacency)
- `name_fictional`: a small curated list for the "James Bond" class, which frequency cannot catch
  because both parts are ordinary names. Roughly 200 entries of fictional characters and public
  figures. This is the one piece with a manual asset, and it needs a periodic review to stay useful.
- `name_matches_email_local_part` as a positive plausibility signal rather than a negative one.

**BUILT AND MEASURED 2026-08-06. Negative as model features, actionable as a rule.**
(`build_name_plausibility.py`, `exp_name_plausibility.py`, ledger row `area2`.)

As features on top of the canonical baseline: pooled 0.8695 to **0.8688**, decidable 0.8172 to
**0.8163**, forward 0.8912 to **0.8900**. All within fold noise and all slightly down. The family alone
scores 0.558. The predicted "floor under precision at the bottom" did not materialise either: the
baseline's bottom decile is already 0.7% corroborated, so there was no floor left to install.

The reason is arithmetic rather than conceptual. The sharp flags fire on 4 to 48 rows each out of
20,480. A gradient-boosted tree cannot learn a split from that, no matter how clean the signal is.

But the signal is clean, and the model does not have it. The five high-precision flags (both names
unknown, first equals last, placeholder token, tripled character, fictional name) union to **91 users at
12.1% corroboration versus a 59.4% base**, and only **19.8% of those 91 sit in the baseline's bottom
decile**. So four out of five obviously-implausible signups are being scored as ordinary by the current
model. That is a rule, not a feature: route them at stage 1, exactly the shape already used for
`br_normalized_position_type = missing`. The wider rule that also takes either-name-unknown reaches 655
users at 35.4%, which is below base but not clean enough to hard-route; leave it as a band adjustment or
leave it out.

**Scope correction from the data profile: this filter is aimed at the wrong population.**
Rule-implausible share is **5.46% of the 40,147 users outside** the office-declared completed-election
population (2,194 users) against **0.44% inside** it (91 users), a 12x difference. First name unknown
6.75% versus 1.30%, both names unknown 4.39% versus 0.23%, first equals last 3.50% versus 0.12%. Junk
signups concentrate exactly in the segment the model does not cover, which also explains the null
feature result: the experiment tested the filter on the cleanest 34% of the user base. The name filter
belongs in **intake triage across all Win signups**, not in the predictor's analysis population.

Contamination note, because the question came up: frequency features are computed from the voter file
and are label-free; the token and fictional-name lists were written a priori, never by inspecting our
own users' names; and no flag was selected on the label. The keyboard-run flag was **kept** despite
firing on 18 users at 66.7% corroboration, above base, precisely because dropping it on that basis
would have been selection on the outcome. The discipline is written into the script docstring so the
next session does not quietly tune the lists against the labels.

### 4.8 Amplitude post-signup product behavior (pass A, build the scaffold now)

Thin today, and that is a volume problem rather than a data-model problem. The surface is large and
getting larger: **526 event types, 322 of them Win, 21.75M events**, with the Win voter-data family
instrumented continuously since 2025-05-28. Forward coverage is far better than historical: 92.3% of
the 2026 in-cycle scoring set has browser presence versus 44% of post-2025-05 signups overall. So the
build decision should be made on where this is going, not on where it is.

**Design the scaffold at the family grain, not the event grain.** `int__amplitude_event_catalog`
already classifies every event type into a family (`win_voter_data`, `win_voter_outreach`,
`win_candidate_profile`, `win_pro_upgrade`, `win_dashboard`, `win_onboarding`, `win_contacts`,
`win_briefings`, `navigation`, `serve`, `amplitude_autotrack`, `other`). Emitting per-family counts,
first-touch timestamps, and depth ratios gives roughly 15 stable columns instead of 526 brittle ones,
survives event renames the way the provenance work intended, and lets new event types join an existing
family without a feature-schema change. That is what "ready to ingest as the model changes" should mean
concretely.

**The download-and-leave hypothesis, measured.** Signups on or after 2025-05-28 (n = 12,875), archetype
by whether the user ever fired `Download Voter File Success` and whether anything non-voter-data
followed more than 7 days later:

| Archetype | n | Corroboration rate | Avg events | Avg active days |
|---|---|---|---|---|
| no browser events at all | 7,232 | 73.3% | 0 | 0 |
| browser events, never downloaded | 5,029 | 52.8% | 71 | 2.6 |
| **downloaded and left** | **81** | **71.6%** | 305 | 6.3 |
| downloaded and stayed | 533 | 84.6% | 716 | 17.6 |

The hypothesis is not supported at current volume. Download-and-leave is 81 users, 0.6% of the
observable window, and they corroborate *above* the 59.4% base rate, not below. The read is that they
are real candidates who got what they came for, a lower-quality slice of a high-quality group rather
than a noise archetype.

Two caveats that matter before anyone acts on that. Downloading is Pro-gated (there is an explicit
"Pro user can not download voter file page" event), so the download population is selected on Pro,
which itself correlates with being real. And 81 users cannot carry a conclusion. What this establishes
is the measurement, now scripted as the `download_archetype` probe, so the same question re-answers
itself as volume grows rather than being re-litigated from scratch.

The other thing this table shows is worth its own follow-up: the no-browser-events group is 56% of the
observable window and corroborates at 73.3%, well above base. That is the parked 2025-wave
CRM-provenance finding reproducing itself, and it is the single largest unexplained block in the
population.

### 4.9 Sales and CRM provenance (92.8% coverage)

`mart_analytics.prospects` on `gp_user_id`. This is where the parked no-browser-presence finding gets
resolved rather than guessed at.
- `sales_touch_before_signup` (37.2%), `days_from_first_sales_touch_to_signup`, `is_self_filer_lead`
  (63.1%), `was_called`, `connected_calls`, `distinct_owners`, `lead_status` (8.9%)
- **L, and this is the important one.** If sales sources leads from BallotReady rosters, then
  "was contacted before signup" implies "BR already knew about this person", which implies the label.
  That is the same circularity the parking lot flagged for the no-browser cohort. Required check
  before any of this becomes a feature: for pre-signup sales touches, determine the lead's origin
  (HubSpot `hs_analytics_source` / `candidate_id_source` / TechSpeed import provenance) and measure
  corroboration rate by origin. If BR-sourced leads dominate, the whole family is a coverage proxy and
  must be dropped from pass B, kept only as a pass-A diagnostic.

### 4.10 Race-context priors and cohort ratios (items 5 and 6)

- `race_prior_corroboration_rate`: historical corroboration rate by `br_normalized_position_type`,
  by state, and by (state, office level), computed **only from cycles strictly before the row's
  election year**, and computed inside the CV fold to avoid target leakage. Without both guards this
  is target encoding and it will look excellent out of the box and fail forward.
- `n_br_candidacies_by_deadline` opponents-only rebuild (already parked, one-line fix at rebuild).
- `voter_count_per_seat`, `filing_fee_per_voter`, and the ratio pass generally: normalize each raw
  count by its race, state, and era cohort median. Cheap once the raw features exist, and it is the
  right answer to the era drift the 0b audit documented.

### 4.11 Sources deliberately left out

- **Segment** (172 tables): the same behavior reaches us through Amplitude, so importing it separately
  would double-count. Its own coverage is also only 2.6% of the training population.
- **Stripe** (72 tables): payment is downstream of Pro, so post-signup by nature.
- **HubSpot fields that duplicate the mart, Amplitude, or L2**: see the one-concept-one-source table in
  4.3. Only first-touch source, channel group, and first referrer survive that filter.
- **`ballotready_s3_recruitment_v1`**: BR recruitment records mean BR is already tracking the person.
  Direct coverage-proxy leakage. Interesting for Area 1 label work, not for features.
- **`model_predictions.viability_*`**: parked pending the PIT deep dive, unchanged.
- **Census and ACS**: available and joinable via place and district, but it duplicates what L2 district
  aggregations already give at finer grain. Low marginal value.

## 5. Raising match rates

Four separate matching problems, in descending order of what they cost us today.

### 5.1 HubSpot: pick the key that matches the grain

No match-rate problem once the grain is right. Candidacy-grain fields: `hubspot_id = companies.id`,
96.6%. Person-grain fields: the ER bridge (`mart_civics.candidate.prod_db_user_id`) at 89.6%, or email
at 99.6%. The only real gap is `goodparty_user_id` on HubSpot contacts, which is populated for 27.7% of
our population; backfilling it would give a direct product-to-contact key and is worth a small dbt
ticket, but it is not blocking, because the bridge and email both already work.

### 5.2 L2 person match: 78.6% any, 41.2% unique, and no existing matcher to reuse

**Checked, and the answer is no: nothing in the warehouse matches an L2 person to one of our people.**
- `mart_civics.person_identifiers` resolves six sources and L2 is not among them: ballotready
  (463,021 records), hubspot (336,356), ddhq (102,150), techspeed (74,566), gp_api (70,374),
  techspeed_officeholder (30,289).
- matcha, the Splink pipeline, has exactly three entity types: `candidacy_stage`, `elected_official`,
  `election_stage`. No L2 or LALVOTERID reference anywhere in the repo.
- Of the 269 warehouse tables carrying `LALVOTERID`, not one also carries a `gp_person_id`,
  `user_id`, `candidate_id`, or email. The voter-file world and the person world do not touch.
- The only L2 matcher that exists is **district grain**, not person grain:
  `model_predictions.llm_l2_br_match_*` (three dated runs) matches L2 district names to BR positions,
  with `l2_br_match_overrides` and `l2_manual_district_assignments` as manual escape hatches.
- `people-api-loader` does push L2 voters into people_api for Serve, but that is a load, not a match.

So this is genuinely new matching work. That does not mean writing it from scratch: matcha is the house
engine and the right move is to reuse it rather than hand-roll fuzzy logic in this project. Two options
worth deciding between before any code:
1. **Add a fourth matcha entity type** (`voter_person` or similar) with gp_api users and L2 as the two
   sources. Correct, reusable by Serve and by the ICP work, and it puts the match under the same
   auditing and threshold tooling as the rest of the ER. Heavier, and it belongs to the data platform
   rather than to this predictor.
2. **Borrow matcha's config pattern for a project-local match**: same blocking rules and comparison
   levels as `configs/candidacy.py`, run once, emit a `user_id -> LALVOTERID` parquet with a score.
   Fast, gives Area 2 its features now, and doubles as the prototype that justifies option 1.

Recommendation: option 2 now, propose option 1 as its own ticket, since a reusable person-to-voter
bridge is worth more to the org than to this model. Either way, the ambiguity levers below are what
the matcher needs to encode.

The gap is ambiguity, not absence. 37.4% of the population matches 2 or more voter records, and only
21.4% match none at all. Levers, in expected-yield order:

1. **Disambiguate with zip.** We hold `user_zip` for 99.8% of the population and L2 has
   `Residence_Addresses_Zip`. Requiring or preferring a zip match should convert a large share of the
   4,141 two-to-five-match rows into unique matches. Measure the conversion before assuming it.
2. **Nickname expansion.** `dbt.nicknames` (name1, name2, relationship) already exists in the
   warehouse. Bill/William, Kate/Katherine. This attacks the 21.4% no-match bucket.
3. **Middle name and suffix.** L2 carries `Voters_MiddleName` and `Voters_NameSuffix`; use them to
   break ties rather than to filter.
4. **Last-name variants.** Hyphenated and married names, plus a bounded edit distance on last name
   within the same zip. Keep it inside a geographic block so it stays cheap and precise.
5. **Nationwide instead of state-scoped.** Matching outside the declared state both raises the match
   rate and creates a genuinely new feature: registered somewhere other than where you are running.
6. **Age filter as precision guard.** With HubSpot `birth_date` (6%) or the position's `minimum_age`,
   drop implausible matches.

Design rule for all of it: emit `l2_match_confidence` and `l2_match_ambiguity` as features rather than
silently collapsing to a best guess. The model can then learn that ambiguity itself is informative.

### 5.3 L2 district match: the 0b finding, unchanged and still worth fixing

The 0b audit established that 79% of the 1,154 unsized ICP rows (667 positions) are matched by the
LLM but emit an `l2_district_name` with no corresponding row in `int__l2_district_aggregations`, a
name-normalization problem needing no new L2 data. `dbt.l2_br_match_overrides` and
`dbt.l2_manual_district_assignments` already exist as the intended escape hatches. Fixing this closes
the `voter_count` gap, which is the same gap as the ICP gap.

### 5.4 BR position coverage: 88%, and the residual is the tautology bucket

18,018 of 20,478 join to a BR position, and that 88% is the same decidable population already
established (missing `br_normalized_position_type` is 100% nested in missing BR arrivals, 1.2%
corroborated, rule-decided rather than modeled). So there is no match-rate work here. Any new BR
position feature must carry the same sentinel discipline as `n_br_candidacies_by_deadline`: NaN plus
an explicit indicator when the position is absent, never a zero that the model can read as a coverage
proxy.

## 6. Suggested build order

Cheap-first, and each stage is independently ledgerable against the 0.8695 pass-B baseline.

Pass-B track, which is what feeds the analysis filter:

1. **Name plausibility** (100% coverage, no match needed, two small frequency tables). Cheapest new
   axis, and it is the only one that directly answers "is this even a person".
2. **BR position attributes** (one join, 88%, entirely new). Highest AUC value per hour.
3. **gp_api user leftovers plus creation-time `details` keys** including `knowRun` and `pledged` and
   the product-side `filingPeriodsEnd`. Add the details-mutability check on the 2026-04-13 onward CDC
   window while doing it.
4. **HubSpot contact-grain first-touch attribution only** via `candidacy.hubspot_contact_id` (99.9%).
   Three fields, not the family.
5. **L2 person match plus the haystaq shortlist**, one build rather than two: exact match first to lock
   in the measured 10-point no-match-versus-matched gap, then attach the dozen shortlisted haystaq
   columns for free on LALVOTERID. Decide the matcha reuse question (5.2) before writing matching code,
   and justify the zip and nickname passes on attribute correctness rather than on AUC.
6. **Sales provenance leakage audit**, then the `prospects` features if and only if the audit clears.
7. **Race priors and the ratio pass**, fold-internal and time-disciplined.
8. **HubSpot company grain**, only for the product-versus-sales office disagreement feature.

Pass-A track, which runs in parallel and never enters the filter:

9. **Amplitude family-level behavior scaffold** (4.8). Build it now at the family grain so the columns
   are stable as event types accumulate, keep it in pass A, and re-run `download_archetype` as volume
   grows. Also the natural home for the Area 1 anti-pattern archetype work.

Dropped: Amplitude pre-signup device merge, Segment as a separate source, details-blob history
reconstruction, and every HubSpot field that duplicates something we already join.

Sequencing against DATA-2235: every feature above is a `user_id`-keyed or `br_position_id`-keyed side
table, exactly like `geo_features.parquet`, so nothing here is invalidated by the working-set rebuild.
Build and read signal now against ws `6fd10aa234d3`, restate the ledger once after the rebuild and
fold re-freeze.
