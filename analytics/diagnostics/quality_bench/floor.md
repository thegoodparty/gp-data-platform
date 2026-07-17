# Working context (common floor)

You are answering a product analytics question against GoodParty's Databricks
warehouse. Everything you need to connect is below. Answer the question you are
given; do not ask clarifying questions (no user is present) — resolve ambiguity
yourself and state every resolution in the assumptions ledger.

## Databricks access

Auth is already configured on this machine (Databricks CLI profile). Run SQL
from Python like this:

    import sys; sys.path.insert(0, "{{LIB_PATH}}")
    import databricks_conn as dbc
    df = dbc.run_query("select 1")

Run Python via: `uv run --project {{UV_PROJECT}} python <script.py>`
The warehouse catalog is `goodparty_data_catalog`; analytics marts live in the
`dbt` schema (query as `goodparty_data_catalog.dbt.<table>`).

## Table inventory (mechanically generated)

| table | description |
|---|---|
| campaigns | Goodparty.org application campaigns. One row per campaign with user information denormalized. Grain: One row per campaign. Limitations: Win/loss outcome data requires future work and is not included in this MVP. |
| clean_states | (no description) |
| deid_voters | De-identified nationwide voter data for MBAN Analytics program (2026). This dataset provides comprehensive voter information with personally identifiable information (PII) removed or hashed to protect individual privacy while maintaining analytical utility for educational purposes. **De-identification Strategy:** - Names (First, Middle, Last) removed - Contact information (phone numbers, email) removed - Phone confidence scores removed as superfluous (CellConfidenceCode, LandlineConfidenceCode) - Physical address lines hashed into household identifiers (mailing_household_hash, residence_household_hash) - Voter ID (LALVOTERID) retained for record linkage - Geographic aggregates (city, state, zip only) retained for analysis - Detailed address components removed (street names, apartment numbers, zip+4, etc.) - Coordinates rounded to 3 decimal places (~100m precision) - Superfluous string versions of numeric fields removed (Age, Estimated_Income_Amount) to retain only integer versions - Sequence/ordering fields removed (SequenceOddEven, SequenceZigZag) **Data Source:** m_people_api__voter (L2 Political nationwide voter file) **Use Cases:** - Voter turnout analysis and modeling - Demographic and geographic voting pattern analysis - Electoral district analysis - Educational analytics projects |
| election_api_race_filing_address_overrides | (no description) |
| fips_codes | (no description) |
| haystaq_issue_tags | (no description) |
| hubspot_call_dispositions | (no description) |
| hubspot_contact_property_columns | (no description) |
| icp_normalized_position_names | (no description) |
| int__amplitude_event_catalog | Read-only catalog of Amplitude events: macro-derived family classification plus volume/recency from the event stream, joined to Amplitude Govern metadata, one row per event_type. Renamed from int__amplitude_event_taxonomy (DATA-2005). Pattern-based so new event_types in known families classify automatically. Does not compute a staleness verdict; code-presence provenance lives as a CSV in the omni repo (packages/runbooks/scripts/python/instrumentation_data/amplitude_event_provenance.csv, DATA-2014) and the relevance / up-to-dateness judgment is made at check time by an agent or the health monitor reconciling that CSV with this table. family is authoritative for classification; govern_category is descriptive metadata only. Grain: one row per distinct event_type. Sources: stg_airbyte_source__amplitude_api_events (behavior), stg_airbyte_source__amplitude_taxonomy_event_type (Govern metadata). |
| int__amplitude_event_taxonomy | Single source of truth for Amplitude event-family classification. One row per distinct event_type, bucketed into a product-feature family via pattern matching (amplitude_event_family macro). Replaces event-type logic previously duplicated across int__amplitude_win_activity, int__amplitude_user_milestones, the analytics helper, and the runbook (DATA-1945). Pattern-based so new event_types in known families classify automatically; first_seen_date enables drift control via WHERE first_seen_date <= '<cutoff>'. Grain: one row per distinct event_type. Source: stg_airbyte_source__amplitude_api_events. |
| int__amplitude_serve_activity | User x month intermediate aggregating Serve MAU activity from Amplitude. Filters to canonical Serve MAU definition: Viewed event on /dashboard/polls with Serve Activated = true, excluding @goodparty.org. Grain: One row per user_id x activity_month_year. Source: stg_airbyte_source__amplitude_api_events. |
| int__amplitude_user_milestones | User-grain intermediate model that aggregates milestone events from stg_airbyte_source__amplitude_api_events into one row per user_id for analytics.users metrics. Supports Onboarding CVR, Activated, and Active Candidates from a single Amplitude scan. |
| int__amplitude_win_activity | User x month intermediate aggregating Win product engagement from Amplitude. Captures campaign sends (Voter Outreach - Campaign Completed), and dashboard views (Dashboard - Candidate Dashboard Viewed). Grain: One row per user_id x activity_month_year. Source: stg_airbyte_source__amplitude_api_events. |
| int__amplitude_win_activity_weekly | User x week intermediate aggregating Win product engagement from Amplitude. Weekly analog of int__amplitude_win_activity (monthly). Substrate for retention modeling, cohort analysis, and point-in-time churn features where the ~12-week typical Win-account engagement window argues for a finer cadence than monthly. Grain: One row per user_id x week_start_date. Week boundaries follow date_trunc('week', ...): Monday-start, Sunday-end. week_end_date is exposed directly so downstream point-in-time features can use it as asof_date without further computation. Timezone semantics: event_time is in UTC; date_trunc('week', ...) is ISO-Monday-anchored. Activity at Sunday 23:30 UTC (Monday morning in many non-UTC locales) is bucketed into the week containing that Sunday, not the user's local Monday. Fine for retention modeling at weekly cadence; flagged so reviewers don't second-guess edge-of-week boundaries. Source: stg_airbyte_source__amplitude_api_events. |
| int__ballotready_candidacy | This model retrieves and processes candidate data from the CivicEngine API since the source candidacies_v3 csv file does not contain all the fields needed. |
| int__ballotready_candidate_identity | Canonical BallotReady candidate identity inputs, one deterministic row per br_candidate_id. int__civics_candidate_ballotready and int__civics_candidacy_ballotready both hash these columns for gp_candidate_id, so a person resolves to the same id in both models (previously they picked divergent per-row vs rolled-up contact values and orphaned candidates whose email varied across candidacy rows). Grain: One row per br_candidate_id. |
| int__ballotready_endorsement | This model retrieves and processes candidate endorsement data from the CivicEngine API. It contains endorsement information for political candidates, including their endorsements from various organizations. |
| int__ballotready_filing_period | This model retrieves and processes filing period data from the CivicEngine API. It contains filing period information for political candidates, including their endorsements from various organizations. |
| int__ballotready_geofence | This model retrieves and processes geofence data from the CivicEngine API. |
| int__ballotready_issue | This model retrieves and processes issue data from the CivicEngine API: https://developers.civicengine.com/docs/api/graphql/reference/objects/issue It is used to retrieve the issue data for the [`stance`](https://developers.civicengine.com/docs/api/graphql/reference/objects/stance) table. |
| int__ballotready_normalized_position | This model retrieves normalized position data from the CivicEngine API. It uses the `position_id` from the position table to retrieve ids on which to query the normalized position data. |
| int__ballotready_party | This model retrieves and processes candidate endorsement data from the CivicEngine API. It contains endorsement information for political candidates, including their endorsements from various organizations. |
| int__ballotready_person | This model retrieves and processes person data from the CivicEngine API. |
| int__ballotready_position_election_frequency | This model retrieves and processes position election frequency data from the CivicEngine API. See https://developers.civicengine.com/docs/api/graphql/reference/objects/position-election-frequency |
| int__ballotready_position_to_place | Intermediate model that maps positions to places from the BallotReady API |
| int__ballotready_stance | This model retrieves and processes candidate stance data from the CivicEngine API. It contains stance information for political candidates, including their positions on various issues. |
| int__civics_candidacy_2025 | Historical archive of candidacies from the general mart with elections on or before 2025-12-31. Snapshot from 2026-01-22. |
| int__civics_candidacy_ballotready | BallotReady candidacies transformed into civics mart candidacy schema. Grain: One row per candidacy (candidate + position + election year). Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections), rolled up from race-level to candidacy-level by grouping on br_candidate_id + br_position_id + br_election_id. |
| int__civics_candidacy_ddhq | DDHQ candidacies transformed into civics mart candidacy schema. Grain: One row per candidacy (candidate + position + election year). Derived from int__civics_candidacy_stage_ddhq, rolled up from candidacy-stage grain with stage-specific dates and results. |
| int__civics_candidacy_gp_api | Product Database campaigns transformed into the civics mart candidacy schema. Grain: one row per latest-version campaign with a non-null election_date. gp_candidate_id uses the user's derived state (most recent non-demo campaign), not the per-campaign state, so users with campaigns in multiple states (or demo-only users with null user_state) keep referential integrity to int__civics_candidate_gp_api. |
| int__civics_candidacy_stage_2025 | Historical archive of candidacy stages (primary, general, general runoff) from elections on or before 2025-12-31. Grain: one row per (gp_candidacy_id, stage_type) where the candidacy carries a non-null date for that stage. DDHQ is an optional attribute join — HubSpot-only candidacies flow through. |
| int__civics_candidacy_stage_ballotready | BallotReady candidacy stages transformed into civics mart candidacy_stage schema. Grain: One row per candidacy stage (each BallotReady S3 candidacy row is a stage). Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections). |
| int__civics_candidacy_stage_ddhq | DDHQ candidacy stages — the foundational DDHQ intermediate model. Grain: One row per candidacy stage (candidate + race), 1:1 with staging rows. Reads from staging, computes all five GP IDs, and carries through all columns needed by the four downstream DDHQ models (candidate, candidacy, election_stage, election). |
| int__civics_candidacy_stage_gp_api | Product Database campaigns × BR election_stage rows for same position+year. Grain: one row per (campaign × BR election_stage). Campaigns without a ballotready_position_id or without a matching BR stage are dropped. Stage-level election results (is_winner, election_result, votes_received) are null — Product DB's did_win is candidacy-grain, not stage-grain. |
| int__civics_candidacy_stage_techspeed | TechSpeed candidacy stages transformed into civics mart candidacy_stage schema. Grain: One row per candidacy stage (candidate + election stage). Source: stg_airbyte_source__techspeed_gdrive_candidates, unpivoted into primary/general stage rows. Each candidate with both dates produces two rows. Won/Unopposed election results mapped to general stages when available. |
| int__civics_candidacy_techspeed | TechSpeed candidates transformed into civics mart candidacy schema. Grain: One row per candidacy (candidate + election combination). Source: stg_airbyte_source__techspeed_gdrive_candidates Key design decisions: - UUID fields aligned with int__hubspot_companies_w_contacts_2025 for cross-source consistency - br_candidacy_id is NULL; will be populated via COALESCE when TS adds it - candidate_code is the linkage key to BallotReady data - No HubSpot dependency - Election-level attributes (population, seats, etc.) NOT included — those belong in election table |
| int__civics_candidate_2025 | Historical archive of candidates from the general mart with elections on or before 2025-12-31. Snapshot from 2026-01-22. |
| int__civics_candidate_ballotready | BallotReady candidates transformed into civics mart candidate schema. Grain: One row per unique person (deduplicated on gp_candidate_id). Source: stg_airbyte_source__ballotready_s3_candidacies_v3 (2026+ elections), augmented with int__ballotready_person for contact/URL data from API. |
| int__civics_candidate_ddhq | DDHQ candidates transformed into civics mart candidate schema. Grain: One row per unique person (deduplicated on gp_candidate_id). Derived from int__civics_candidacy_stage_ddhq. gp_candidate_id is the person id from the stage model; cross-source dedupe happens via shared person groups, not attribute hashes. |
| int__civics_candidate_gp_api | Product Database (gp_api_db) users transformed into the civics mart candidate schema. Grain: one row per user with campaign_count > 0. gp_candidate_id is the person id from int__civics_person_canonical_ids (record_key gp_api\|user_id), with a deterministic self-mint guard. |
| int__civics_candidate_techspeed | TechSpeed candidates transformed into civics mart candidate schema. Grain: One row per unique person (deduplicated on gp_candidate_id). Source: stg_airbyte_source__techspeed_gdrive_candidates gp_candidate_id is the person id: min gp_person_id over the stage-stripped candidate_code's clustered records, self-mint for codes absent from ER (own-person semantics). |
| int__civics_elected_official_ballotready | BallotReady office holders, term-grain. Carries both term-grain and person-grain canonical UUIDs (gp_elected_official_term_id and gp_elected_official_id) so downstream models can consume them without regenerating salted UUIDs. ICP flags are joined upstream via int__icp_offices on br_position_id (Win and Win-Supersize gated by candidacy election_day). Grain: One row per office-holder term. Source: stg_airbyte_source__ballotready_s3_office_holders_v3. |
| int__civics_elected_official_ballotready_person | BR-only person-grain rollup of int__civics_elected_official_ballotready. One row per br_candidate_id, populated with the person's latest term snapshot. Plays the same role as int__civics_candidate_ballotready in the merge (BR-side input to a person-grain mart), but the grain mechanics differ: this is a latest-term window over term-grain BR data, not a natural person grain. Grain: one row per person (br_candidate_id). Latest-term ordering: term_start_date desc nulls last, term_end_date desc nulls last, updated_at desc, br_office_holder_id desc. Term-scoped attributes (candidate_office, office_level, office_type, state, city, district, mailing_*, tier, party_affiliation) take the latest term's value. Multi-term people with cross-term variation: - 8,400+ multi-office people - 7,300+ multi-office_type people Consumers needing per-term accuracy must query elected_official_terms. ICP flags (is_win_icp, is_serve_icp, is_win_supersize_icp) are intentionally NOT included on this intermediate. Per Hugh's commit 8c22079, ICP is position-scoped and a latest-term scalar misrepresents the data. ICP lives at term grain only (int__civics_elected_official_ballotready, elected_official_terms). Audit trail: selected_gp_elected_official_term_id and selected_br_office_holder_id expose which term row was chosen as "latest," so debugging "why this person got office X" doesn't require re-running the window logic. Vacancy terms (br_candidate_id IS NULL) are filtered out. |
| int__civics_elected_official_canonical_ids | Deterministic crosswalk: TechSpeed ts_officeholder_id ↔ BallotReady canonical IDs (term-grain UUID, person-grain UUID, raw BR keys). Grain: one row per distinct numeric ts_officeholder_id that deterministically matches a BR br_office_holder_id. This is a key-mapping model only — it does NOT assert that ts_officeholder_id is a clean person identifier (see ts_officeholder_id_is_reused). Reads TS staging directly to stay decoupled from the prematch-preserving TS intermediate and to compute reuse against the same delivery-dedup population. |
| int__civics_elected_official_ddhq | (no description) |
| int__civics_elected_official_ddhq_matched_votes | DDHQ general-winner votes matched to each elected official via the matcha elected_official entity resolution: a DDHQ winner that shares a Splink cluster (and surname) with the official's BallotReady term and/or gp-api office carries that official's own winning votes. Long format: one row per matched record, keyed by EITHER br_office_holder_id (BR term) OR gp_api_elected_office_id (gp-api office), the other being NULL. BR rows feed elected_official_terms (every official, not just gp-api ones); gp-api rows feed the election_api support score, including gp_api-only offices with no BR term. |
| int__civics_elected_official_gp_api | Product Database elected offices transformed into the civics mart elected_officials schema. Grain: one row per gp_api_db_elected_office row whose campaign_id resolves in the campaigns mart (orphans filtered). Sourced from gp_api_db_elected_office (authoritative) — campaigns.did_win is stale for most winning campaigns. |
| int__civics_elected_official_gp_api_bridge | Term-grain bridge from gp-api elected_office records to BR+TS terms, derived from the EO Splink cluster output. One row per gp_api elected_office record (deduped); also unique on br_office_holder_id after pass-2 collision suppression. The bridge resolves two ambiguities: 1. Multi-term: when a gp_api record's Splink cluster contains more than one BR term, pass 1 picks the BR term whose term_start_date is closest to gp_api sworn_in_date. 2. Bridge collision: when two gp_api records in the same Splink cluster pick the same BR term as their best match, pass 2 keeps the closer of the two; the loser falls back to a self-key in the person rollup and appears as a gp_api-only mart row. The bridge collision audit custom SQL test reports the dropped count. `hubspot_company_id` is sourced from `campaigns.hubspot_id` via `gp_api_campaign_id` join; deliberate alias of `hubspot_id` to make CRM linkage clearer at the term/campaign grain. |
| int__civics_elected_official_gp_api_person | Person-grain rollup of gp-api elected officials. One row per gp_api_user_id. Adopts BR's gp_elected_official_id via the bridge for matched users (so the mart's 3-way FOJ on gp_elected_official_id collapses correctly). Unmatched users get a salted UUID specific to gp_api ('elected_official_gp_api_user' salt) so they don't collide with BR/TS canonical IDs. `hubspot_contact_id` is renamed and cast in staging (`stg_airbyte_source__gp_api_db_user`). |
| int__civics_elected_official_techspeed | TechSpeed office holders transformed into the civics mart elected_officials schema. Grain: One row per officeholder-position (current snapshot, deduplicated). Source: stg_airbyte_source__techspeed_gdrive_officeholders. |
| int__civics_elected_official_techspeed_person | TS-side person-grain rollup. Adopts BR canonical IDs via int__civics_elected_official_canonical_ids, suppresses reused-ID contamination, rolls up phone/email per-field (FIRST_VALUE IGNORE NULLS), and detects is_incumbent conflicts at canonical grain. Consumed by the elected_officials person mart in a full-outer-join + coalesce against int__civics_elected_official_ballotready_person. Grain: one row per canonical_gp_elected_official_id. PR #333 data: 27,827 rows. Canonicals whose TS contributions are entirely from reused ts_officeholder_ids (53 in PR #333 data) are EXCLUDED — no clean TS rows means no TS contribution, and the person mart's full-outer-join correctly yields BR-only. Reused-ID suppression: 86 IDs flagged in the crosswalk; - 53 reused-only canonicals dropped entirely - For canonicals with both reused and clean rows, reused rows are suppressed; clean rows still contribute via per-field rollup - Per-field rollup preserves: 107 phones (across 908 multi-phone canonicals) and 1,097 emails (across 210 multi-email canonicals) vs single-row dedup Conflict handling: when contributing TS rows disagree on is_incumbent for a canonical (350 affected canonicals in PR #333 data), the value is NULL'd and ts_incumbent_conflict is set true. TS-only fallback is unsolved (zero rows today). The unmatched-TS singular test on the crosswalk blocks the build if any appear. |
| int__civics_election_2025 | Historical archive of elections from the general mart with election dates on or before 2025-12-31. Snapshot from 2026-01-22. |
| int__civics_election_ballotready | BallotReady elections transformed into civics mart election schema. Grain: One row per election (position + election date). Source: Rolled up from int__civics_election_stage_ballotready, which sources from BallotReady API race, election, and position tables. |
| int__civics_election_ddhq | DDHQ elections transformed into civics mart election schema. Grain: One row per election (position + election year). Derived from int__civics_candidacy_stage_ddhq, representative-row approach preferring general stage. |
| int__civics_election_stage_2025 | Historical archive of election stages (primary, general, general runoff) from elections on or before 2025-12-31. Grain: one row per (gp_election_id, stage_type). Seeded from candidacy dates so HubSpot-only stages are represented; DDHQ race attributes are left-joined when a match exists. |
| int__civics_election_stage_ballotready | BallotReady election stages transformed into civics mart election_stage schema. Grain: One row per election stage (position + election + stage type). Source: BallotReady API race, election, and position tables (2026+ elections). This model is the foundation for int__civics_election_ballotready, which rolls up stages into elections. |
| int__civics_election_stage_ddhq | DDHQ election results transformed into civics mart election_stage schema. Grain: One row per DDHQ race (ddhq_race_id). Derived from int__civics_candidacy_stage_ddhq, aggregated from candidate-level to race-level. |
| int__civics_election_stage_techspeed | TechSpeed election stages transformed into civics mart election_stage schema. Grain: One row per election stage (race + stage type). Source: stg_airbyte_source__techspeed_gdrive_candidates, unpivoted from candidate-level into primary/general stage rows, then aggregated to race-level. gp_election_stage_id uses a composite key (NOT br_race_id) because a TechSpeed candidate has one br_race_id but two stage dates. br_race_id is stored as a reference column only. |
| int__civics_election_techspeed | TechSpeed elections transformed into civics mart election schema. Grain: One row per election (position + election year). Source: stg_airbyte_source__techspeed_gdrive_candidates, grouped by election-defining fields. Enriched with population, filing_deadline, and contested status from the source data. |
| int__civics_er_canonical_election_stages | Direct election-stage ER crosswalk: maps DDHQ / TechSpeed election stages that clustered with a BallotReady race (gp-data-matcha election_stage entity) to BR's canonical gp_election_stage_id. Broader race-level coverage than int__civics_er_canonical_ids's candidacy-derived canonical_gp_election_stage_id (links races even without a matched candidacy). BR-anchored clusters adopt BR's id; non-BR cross-source clusters (DDHQ <-> TS, no BR member) adopt a representative member's id so they still collapse. Single-record clusters keep their natural id. Grain: one row per non-BR provider election stage (source_name + gp_election_stage_id). Note: DDHQ and TS can share a natural gp_election_stage_id for the same race, so the id is unique only within a source. A small number (~41) of such shared ids map to different BR canonicals (the matcher anchored the DDHQ vs TS record to different BR races); consumers join per-source so the mapping stays deterministic. |
| int__civics_er_canonical_ids | Entity resolution crosswalk: provider raw keys → canonical gp_* IDs (BR's cluster-derived ids on BR-anchored clusters, the earliest-member mint on non-BR clusters). Wide schema covering TechSpeed (keyed by ts_source_candidate_id + ts_stage_election_date), Product DB (keyed by gp_api_campaign_id + gp_api_stage_election_date), and DDHQ (keyed by ddhq_candidate_id + ddhq_race_id). Provider columns are null on rows from other providers. Candidate grain is not here: gp_candidate_id is the person id, resolved per provider via int__civics_person_canonical_ids. |
| int__civics_minted_candidacy_ids | Minted gp_candidacy_id per clustered candidacy-stage record. One row per candidacy-stage unique_id. The id is the salted-hash of the cluster member earliest by first_seen_at (ties: source_name, source_id), shared by all members, so it is stable under cluster churn. Clusters are single-stage (election dates may differ within matcha's 10-day window), so consuming provider models roll this up to candidacy grain (min over their candidacy grouping); records in no cluster mint from themselves via the provider self-mint fallback. |
| int__civics_minted_election_stage_ids | Minted gp_election_stage_id per clustered election-stage record. One row per election-stage unique_id. Same earliest-member rule as the candidacy mint; election-stage clusters are already at stage grain, so no rollup is needed. Records in no cluster mint from themselves via the provider self-mint fallback. |
| int__civics_person_canonical_ids | Canonical gp_person_id per record. One row per record_key. The id is minted from the group member earliest by first_seen_at (ties: source_name, source_id) via generate_salted_uuid([source_name, source_id], salt=person), so first-in wins uniformly and the id survives a later record joining the group. first_seen_at is computed inline where each record's native id and source timestamp share a row. |
| int__civics_person_edges | Deterministic person edges. One row per typed edge between two record keys (record_key = source_name \|\| '\|' \|\| source_id). Direction-agnostic (record_key_1 <= record_key_2). No new matching: edges derive from native ids (E1 HubSpot<->gp_api, E3 HubSpot->BR candidacy), the elected-official linkages (E4 ts_officeholder->BR, E6 gp_api->BR bridge), candidacy-stage Splink cluster co-membership (E5, hub to the cluster's min record key), and within-source vendor keys (E7). |
| int__civics_person_groups | Person groups. One row per int__civics_person_nodes record_key with its person_group_key: the min record_key reachable over the non-conflicting edges, via min-label propagation across 15 unrolled passes (Spark SQL, no recursion). Pass count exceeds the largest component's diameter with headroom; convergence is tested (penultimate pass == final). had_conflict rolls up E7 conflict edges to the group (both endpoints). |
| int__civics_person_nodes | Person record universe. One row per record_key (source_name \|\| '\|' \|\| source_id) participating in person identity: BR persons (all-time candidacies + officeholder terms), gp_api users, HubSpot contacts, non-reused TS officeholder ids, and TS/DDHQ candidacy-stage records. Also unions in the cluster- and bridge-derived gp_api user ids so every edge endpoint is guaranteed a label seed in the propagation. Deterministically derived, no probabilistic matching. |
| int__civics_position_office_type | One row per BallotReady position with the canonical office_type, derived from the normalized position name (DATA-1972). Source of office_type for positioned candidacies in the civics marts. |
| int__civics_viability_scoring | Viability scores for all candidacies in the civics mart, produced by an MLflow model waterfall (best-available model wins via COALESCE). Reads the candidacy + election marts as leaves; its output is joined onto the downstream candidacy_scored mart, never back onto candidacy (acyclic). Models load by latest registered version from model_predictions (no @prod alias). Grain: one row per gp_candidacy_id. |
| int__ddhq_election_results_clean | Intermediate model that pulls selected properties from DDHQ election results |
| int__ddhq_election_results_embeddings | Intermediate model that creates embeddings for the election results |
| int__ddhq_races | Race-level union of DDHQ reported and upcoming races from the DDHQ Elections gsheet. One row per ddhq_race_id. is_reported is true when the row originates from the reported_races staging table and false when it originates from the upcoming_races staging table. |
| int__district_census_allocation | THE SUBSTRATE (DATA-1992, epic DATA-1359). One row per (census block, district_type, normalized district_name) with the 2020 decennial population allocated to that district in that block: allocated_population = pct_of_geo * block_population, where pct_of_geo is the within-(block, district_type) L2 voter share and the magnitude is census block population (never voter counts). Block grain only; block_group_geoid (LEFT 12) and tract_geoid (LEFT 11) are carried for GROUP BY rollups (coarser grains are never stored as rows, so no cross-grain double-count). Counts only, PII-free. Depends on L2 + census only (no election-api edge). Consumed by people_served and district_census_stats. |
| int__enhanced_place | Intermediate model that enhances the place data from the BallotReady API |
| int__enhanced_place_w_parent | Intermediate model that enhances the place data from the BallotReady API with its parent place |
| int__enhanced_position | Intermediate model that enhances the position data from the BallotReady API |
| int__enhanced_position_w_parent | Intermediate model that enhances the position data from the BallotReady API with its parent position |
| int__enhanced_race | Intermediate model that enhances the race data from the BallotReady API |
| int__er_election_stage_candidacy_clusters | (no description) |
| int__er_prematch_candidacy_stages | Entity resolution prematch table: BallotReady x TechSpeed x DDHQ x GP API candidacy-stages unioned into a standardized schema for Splink probabilistic matching. Grain: One row per source candidacy-stage record (candidate + office + election date). GP API contributes one row per latest-version pledged campaign; BR/TS/DDHQ each contribute one row per (candidate, race/stage). Sources: stg_airbyte_source__ballotready_s3_candidacies_v3 (BR staging), stg_airbyte_source__techspeed_gdrive_candidates (TS), stg_airbyte_source__ddhq_gdrive_election_results (DDHQ), campaigns mart (GP API). The gp_api side resolves election_stage and br_race_id deterministically from PD's pledged ballotready_race_id against BR's race spine (~94% coverage); both fields are null for the ~6% of campaigns where PD didn't set raceId. |
| int__er_prematch_elected_officials | Entity resolution prematch table at term grain. Two sources: ballotready_techspeed (BR term LEFT JOIN canonical_ids LEFT JOIN raw TS with reused-ID guard inside the TS join clause) and gp_api (with raw campaign_office_raw for official_office_name and district extraction). Grain: One row per BR office-holder term (BR+TS source) / one row per gp-api elected_office row (gp_api source). Sources: int__civics_elected_official_ballotready (BR term), int__civics_elected_official_canonical_ids (crosswalk), int__civics_elected_official_techspeed (raw TS), int__civics_elected_official_gp_api (gp-api), with nicknames seed for first_name alias arrays. Unlike the candidacy prematch, elected officials have no election_date or br_race_id. Splink blocking anchors on state + office name + last name and contact info (phone, email). |
| int__er_prematch_election_stages | Entity resolution prematch input for the matcha election_stage entity type. Unions BR / DDHQ / TS race-level records into a standardized schema for Splink matching. One row per source race record. Grain: One row per source race record (election stage). Sources: int__civics_election_stage_ballotready (BR, joined to BR position staging for state), int__civics_election_stage_ddhq (DDHQ; ddhq_race_id can repeat across stages, so source_id uses gp_election_stage_id), int__civics_election_stage_techspeed (TS; state extracted from race_name's `STATE ` prefix). ballotready_position_id is NULL on the DDHQ and TS branches in V1. DDHQ has no native BR position FK, and resolving it requires the election_stage ER clusters that this view itself seeds. first_name_aliases is emitted as an empty array because matcha's shared pipeline.load_and_prepare unconditionally reads the column even though election_stage has no person fields. Output of matcha lands at goodparty_data_catalog.er_source.er_clustered_election_stages and is consumed by int__civics_minted_election_stage_ids. |
| int__general_candidacy | This model creates the candidacies object in the mart layer using the hubspot data |
| int__general_candidacy_clean_for_ddhq | This model creates the candidacies object in preparation of the mart layer using the candidacy data. This intermediate model will be used in the generation of embeddings and eventually LLM calls to match DDHQ election results with the candidacies object. |
| int__general_candidacy_embeddings_for_ddhq | Intermediate model that creates embeddings for the general candidacy data |
| int__general_states_zip_code_range | "Intermediate model that pulls selected properties from HubSpot contacts. To be used to build a general mart of Candidacies." |
| int__geo_id_attributes | Intermediate model that maps geo_id to its components and parent geo_id |
| int__gp_ai_candidacies | Intermediate model that creates the candidacies object in the intermediate layer using the HubSpot data. A "candidacy" is a combination of a candidate and a full election for a specific office in a specific year. This is the format used to start the election match process for GP AI. |
| int__gp_ai_election_match | DEPRECATED (DATA-1834): Frozen snapshot of DDHQ-HubSpot AI election match results from Dec 2, 2025. Previously a Python model that fetched parquet from S3 via the gp-ai API. Splink modelling replaces this pipeline going forward. Kept as a SQL passthrough for backward compatibility with 6 downstream consumers. CLEANUP TODO: To fully remove this model, update these downstream consumers to reference stg_model_predictions__candidacy_ddhq_matches_20251202 directly (or migrate to civics mart / splink data), then delete this file and YAML: - m_general__candidacy_stage.sql (LEFT JOIN) - m_general__candidacy_preview.sql (LEFT JOIN, first in DDHQ>BR>LLM waterfall) - m_general__election_stage.sql (FROM — primary source) - int__civics_candidacy_stage_2025.sql (LEFT JOIN, archive) - int__civics_election_stage_2025.sql (FROM, archive) - mban_candidacy_election_results.sql (INNER JOIN — evaluate deprecation) Also consider removing the 5 older unused snapshot staging models: - stg_model_predictions__candidacy_ddhq_matches_2025{0826,0909,0916,1016,1112} |
| int__gp_ai_start_election_match | Intermediate model that starts the election match process for GP AI |
| int__hs_companies_recent | (no description) |
| int__hs_companies_with_contacts | (no description) |
| int__hubspot_calls | HubSpot engagement calls, deduped to one row per call_id and enriched with the disposition label and outcome_family classification from the hubspot_call_dispositions seed. |
| int__hubspot_candidate_codes | "Intermediate model that generates unique candidate codes by concatenating and cleaning first name, last name, state, and office type from HubSpot contacts. Used for candidate identification and matching." |
| int__hubspot_companies_archive_2025 | Archived HubSpot companies prioritizing 2025 election dates. Selects from snapshot to get historical data with 2025 election dates. |
| int__hubspot_companies_w_contacts_2025 | Archived HubSpot companies joined with contacts from 2026-01-22 snapshot. Upstream source for civics candidacy data. Empty strings in verified_candidate are converted to nulls at this layer. |
| int__hubspot_contact_calls | One row per HubSpot contact_id with at least one logged call. Disjoint per-outcome_family subtotals sum to total_calls. The connected_calls column is a convenience aggregate (pledge + reject + other) and is excluded from the disjoint-sum invariant test. |
| int__hubspot_contacts | "Intermediate model that pulls selected properties from HubSpot contacts. To be used to build a general mart of Candidacies." |
| int__hubspot_contacts_2025 | Archived HubSpot contacts from 2026-01-22 snapshot. Source data for civics mart. |
| int__hubspot_contacts_archive_2025 | Archived HubSpot contacts prioritizing 2025 election dates. Selects from snapshot to get historical data with 2025 election dates. |
| int__hubspot_contacts_w_companies | "Intermediate model that pulls selected properties from HubSpot contacts. To be used to build a general mart of Candidacies." |
| int__hubspot_contest | "Intermediate model that pulls selected properties from HubSpot contacts. To be used to build a general mart of Contest." |
| int__hubspot_contest_2025 | (no description) |
| int__hubspot_prospect_contacts | One row per HubSpot contact, with lifecycle/stage/activation/pledge fields and resolved position-based ICP from int__hubspot_contacts. Does NOT filter by name — unlike int__hubspot_contacts — so prospects with missing names are included. |
| int__icp_offices | ICP offices with voter counts per district (state, district type, and district name) |
| int__l2_block_district_map | One row per (census block, state, district_type, normalized district_name) with the L2 voter count in that intersection (voters_in_block_district) and the per-(block, district_type) voter total (voters_in_block). The block-grain map underlying the district/census substrate (DATA-1992; widened DATA-2013); UNPIVOTs the curated substrate district columns. Block grain is the source of truth for overlap (do NOT count by joining the district-level int__l2_district_aggregations on the normalized name - its key is non-unique and would fan out). |
| int__l2_district_aggregations | Intermediate model that aggregates L2 district data for downstream use. |
| int__l2_nationwide_haystaq_flags | Combined Haystaq issue model flags from all states (loaded from per-state L2 Haystaq flags tables) |
| int__l2_nationwide_haystaq_scores | Combined Haystaq issue model scores from all states (loaded from per-state L2 Haystaq scores tables) |
| int__l2_nationwide_uniform | Combined uniform voter file data from all states |
| int__l2_nationwide_uniform_w_haystaq | Nationwide uniform voter file data with Haystaq flags and scores joined on LALVOTERID |
| int__model_prediction_voter_turnout | Turnout projections data load from model predictions |
| int__place_fast_facts | Intermediate model that adds fast facts to places |
| int__place_fun_facts | Intermediate model that adds fun facts to places |
| int__position_fast_facts | Intermediate model that adds fast facts to positions |
| int__position_fun_facts | Intermediate model that adds fun facts to positions |
| int__race_to_positions | Intermediate model that maps races to positions from the BallotReady API |
| int__serve_active_user | The single source of truth for the "active serve user" behavioral definition (epic DATA-1359), one row per user. A user is an active serve user when they completed Serve onboarding by sending an SMS poll AND have pledged. This is the behavioral half of the People Served cohort; the office half (Serve-ICP) and the internal-email exclusion are applied at int__serve_district_resolution, where the office and owner email live. Reads staging + intermediate only (never an analytics mart) so the cohort flag it feeds stays below the analytics layer, with no dependency cycle into int__serve_block_coverage. "Pledged" is any pledged campaign from source-staging campaigns, which on the serve cohort is identical to the stricter "latest candidacy pledged" notion (proven: zero membership delta). |
| int__serve_block_coverage | The count-once engine (DATA-1993, epic DATA-1359). One row per (census block, served_set) with the count of DISTINCT cohort-served voters and the block's total voters; people_served reads it as count-once(set) = sum(served_voters / total_voters * block_population) over served blocks. Count-once is a distinct-PERSON union across overlapping district types, which the counts-only substrate cannot produce (cross-type voter dedup needs voter IDs), so this model scans int__l2_nationwide_uniform once -- the "scanned, never surfaced" node from TDD 7.1 -- and emits only block-grain counts (PII-free). Scoped to the same district types as the substrate (get_l2_major_district_columns), so count-once <= count-multiple holds. served_set is 'all' (served by any cohort district) or one of the substrate's L2 district types; statewide officials are read from the exact T5 'State' rows in people_served, not here. served_voters_active is the same count restricted to the active cohort (in_people_served_cohort on the resolver); active <= all block-for-block. |
| int__serve_district_resolution | Resolves each serve org to its L2 district: the serve-cohort entry point onto the District/Census substrate (a downstream consumer; election-api quarantined here, not in the substrate). Part of the Constituents Served pipeline (DATA-1988, epic DATA-1359). One row per serve org (organizations mart, organization_type = 'serve'), resolved to the L2 district the official serves: override-first through the org's election-api district record (coalesce(override_district_id, district_id), which bakes in the manual override corrections), with the LLM position->district crosswalk as fallback. Carries the snapshot-stable normalized district join key (L2 names drift between snapshots), the resolution path, and guardrail flags: requires_review (deterministic container-mismatch rules), is_geo_seat (the jurisdiction-vs-electoral-seat semantics question), is_statewide (reported separately downstream), and is_internal_email. Rows are never excluded here; pending cohort decisions are var-driven filters plus flags, and flagged rows flow through for downstream review. Carries in_people_served_cohort (epic DATA-1359): the active People Served cohort gate -- active serve user (int__serve_active_user) AND Serve-ICP office (int__icp_offices, a unique join) AND not internal email -- as a flag, never a row exclusion. |
| int__voter_turnout_lgbm_inference | Nationwide district-level voter-turnout projections from the LightGBM models (DATA-2015). One row per (state, district_type, district_name, election_year, election_code). Replaces the per-state inference models. Projections are known to skew high (calibration caveat from research). |
| int__zip_code_to_br_office | Zip code to BR office mapping |
| int__zip_code_to_l2_district | Zip code to L2 district mapping. One row per (zip_code, state_postal_code, district_type, district_name) with the L2 voter count in that intersection and the total L2 voter count for the zip. No threshold is applied; downstream consumers decide cutoffs. |
| l2_br_match_overrides | (no description) |
| l2_column_classification | (no description) |
| load__l2_haystaq_s3_to_databricks | This model loads Haystaq issue model data from S3 to Databricks. |
| load__l2_haystaq_sftp_to_s3 | This model loads Haystaq issue model data from the L2 SFTP server to S3. |
| load__l2_s3_to_databricks | This model loads data from S3 to Databricks. |
| load__l2_sftp_to_s3 | This model loads data from the L2 SFTP server to S3. |
| m_election_api__candidacy | Mart model that serves the Election API |
| m_election_api__district | Mart model that serves districts to the Election API, part of projected turnout |
| m_election_api__district_top_issues | District-level Haystaq issue scores per L2 district, covering every L2 district with an `is_matched = true` row in the LLM L2-to-BallotReady district match (`stg_model_predictions__llm_l2_br_match_20260126`). Not scoped to a single election cycle — districts with off-cycle offices are included as well. Each row carries four jurisdictional flags (is_local, is_regional, is_state, is_federal) indicating where the issue is actionable. Downstream consumers filter by flag and re-rank within the filtered set. The mart emits the overall `issue_rank` only. Grain: up to one row per (l2_state, l2_district_type, l2_district_name, issue) where the district has at least one voter with a non-null score for that issue. Spark UNPIVOT defaults to EXCLUDE NULLS, so districts with NULL average scores for an issue emit no row for that issue. Issue universe, labels, and flags are sourced from the `haystaq_issue_tags` seed. |
| m_election_api__elected_official_support | Support metrics for the product's elected offices, one row per gp-api elected_office instance (elected_office_id, tied to a user/official). Feeds the Election API Elected_Office_Support table; the product displays support_constituents / total_constituents. support_constituents is taken by precedence: (1) the official's OWN DDHQ winning votes, from the DDHQ winner clustered to the office by the matcha elected_official entity resolution (primary); else (2) a position-level figure from the office's most recent 2026+ GENERAL win in the civics data (multi-seat offices use the top winner by votes). All DDHQ office-name matching lives in the matcher (the prematch official_office_norm key), not a separate supplement here. Only offices with a coherent vote tally and an L2 voter count are present; offices with no support figure are dropped. Grain: One row per elected_office_id. Source note: support_constituents (winning votes) is DDHQ via the matcha cluster (the official's own votes), falling back to the civics 2026+ general-win path; total_constituents is the L2 district voter count from the positions mart. |
| m_election_api__issue | Mart model that serves the Election API |
| m_election_api__place | Mart model that serves the Election API |
| m_election_api__position | Mart model that serves the Election API |
| m_election_api__projected_turnout | Turnout projections data load from model predictions |
| m_election_api__race | Mart model that serves the Election API |
| m_election_api__stance | Mart model that serves the Election API |
| m_election_api__zip_to_position | Maps zip codes to future election positions for the OfficePicker product. Joins the civics election mart with the zip-to-BallotReady-office intermediate model, filtered to elections within two years of the current date. Grain: One row per (zip_code, position_id, election_date). Positions without zip coverage will have a null zip_code. |
| m_general__candidacy | This model creates the candidacies object in the mart layer using the hubspot data |
| m_general__candidacy_preview | This model creates the candidacy preview object in the mart layer using the hubspot data |
| m_general__candidacy_stage | This model creates the candidacy stage object in the mart layer using DDHQ data and matches |
| m_general__candidacy_v2 | (no description) |
| m_general__candidate | This model creates the candidate object in the mart layer using the hubspot data |
| m_general__candidate_v2 | This model creates the candidate v2 object in the mart layer using the hubspot data |
| m_general__contest | This model creates the contest object in the mart layer using the hubspot data |
| m_general__election | This model creates the election object in the mart layer using the hubspot data |
| m_general__election_stage | This model creates the election stage object in the mart layer using DDHQ data |
| m_people_api__district | This model creates the district table in the mart layer using the election_api district table. |
| m_people_api__districtstats | District statistics aggregating voter demographic data per district. Computes bucket distributions for age, homeowner status, education, presence of children, and estimated income range for each district. |
| m_people_api__districtvoter | This model creates the district voter table in the mart layer using the voter table. |
| m_people_api__voter | This model creates the voter table in the mart layer using the hubspot data |
| metricflow_time_spine | Daily time spine for the dbt semantic layer (MetricFlow). One row per calendar day. Required by any project that defines metrics. |
| nicknames | (no description) |
| serve_agent_voters_columns | (no description) |
| snapshot__hubspot_api_companies | (no description) |
| snapshot__hubspot_api_contacts | (no description) |
| snapshot__hubspot_api_deals | (no description) |
| snapshot__int__civics_person_canonical_ids | (no description) |
| snapshot__int__l2_nationwide_uniform | (no description) |
| states_zip_code_range | (no description) |
| stg_airbyte_internal__raw_gp_api_db_campaign | Parsed and deduplicated campaign data from Airbyte's insert-only raw stream. Unlike the standard source table (which only reflects the current state of the product DB), this model preserves historical campaign versions that were overwritten when users reused their campaign for a new election. Grain: One row per distinct campaign version, identified by the combination of campaign id and election context (election date, position, office, state). |
| stg_airbyte_source__amplitude_api_active_users | Staging model for Amplitude Active Users Counts stream - tracks daily active user counts with incremental sync |
| stg_airbyte_source__amplitude_api_annotations | Staging model for Amplitude Annotations stream - contains chart annotations and notes |
| stg_airbyte_source__amplitude_api_average_session_length | Staging model for Amplitude Average Session Length stream - tracks daily average session duration with incremental sync |
| stg_airbyte_source__amplitude_api_cohorts | Staging model for Amplitude Cohorts stream - contains user cohort definitions and metadata |
| stg_airbyte_source__amplitude_api_events | Staging model for Amplitude Events stream - contains individual user events with incremental sync |
| stg_airbyte_source__amplitude_api_events_list | Staging model for Amplitude Events List stream - contains metadata about tracked events |
| stg_airbyte_source__amplitude_taxonomy_event_type | Staging model for the Amplitude Govern taxonomy stream - one row per event_type with human-curated metadata. category is flattened to its name and tags is parsed to an array in this layer. |
| stg_airbyte_source__ballotready_api_election | Election data scan from [`elections` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/elections) |
| stg_airbyte_source__ballotready_api_issue | Issue data scan from [`issues` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/issues) |
| stg_airbyte_source__ballotready_api_mtfcc | MTFCC data scan from [`mtfcc` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/mtfcc). Note that the documentation is incorrect and `createdAt` nor `updatedAt` are not present in the data. |
| stg_airbyte_source__ballotready_api_place | Place data scan from [`places` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/places) |
| stg_airbyte_source__ballotready_api_position | Position data scan from [`positions` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/positions) |
| stg_airbyte_source__ballotready_api_position_to_place | Position data scan from [`positions` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/positions) |
| stg_airbyte_source__ballotready_api_race | Race data scan from [`races` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/races) |
| stg_airbyte_source__ballotready_s3_candidacies_v3 | Candidacies data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_office_holders_v3 | Office Holders data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_recruitment_v1 | Recruitment data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_uscities_v1_77 | US Cities data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_uscounties_v1_73 | US Counties data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ddhq_elections_gsheet_reported_races | Race-level rows from the DDHQ Elections gsheet for races whose results have been reported. One row per ddhq_race_id. |
| stg_airbyte_source__ddhq_elections_gsheet_upcoming_races | Race-level rows from the DDHQ Elections gsheet for races whose results have not yet been reported. One row per ddhq_race_id. |
| stg_airbyte_source__ddhq_gdrive_election_results | Election results data load from ddhq google drive |
| stg_airbyte_source__ddhq_gdrive_election_results_invalid | DDHQ election results rows that fail data quality checks. Captures rows with null candidate_id (non-numeric source values) or race_name that does not start with a recognized state name (e.g. city-prefixed names like "Grand Prairie City Council"). Used for data quality monitoring and investigation. |
| stg_airbyte_source__gp_api_db_annotation | Annotation records (notes, chats, bug reports) attached to EO Meeting Briefing artifacts. One row per annotation. |
| stg_airbyte_source__gp_api_db_annotation_bug_report | raw data load from gp_api_db postgres db from table `annotation_bug_report` |
| stg_airbyte_source__gp_api_db_annotation_note | raw data load from gp_api_db postgres db from table `annotation_note` |
| stg_airbyte_source__gp_api_db_annotation_note_attachment | raw data load from gp_api_db postgres db from table `annotation_note_attachment` |
| stg_airbyte_source__gp_api_db_annotation_review | raw data load from gp_api_db postgres db from table `annotation_review` |
| stg_airbyte_source__gp_api_db_artifact_feedback | Positive/negative feedback submitted on EO Meeting Briefing artifacts. One row per (submitter, briefing, artifact) feedback submission. |
| stg_airbyte_source__gp_api_db_artifact_review | Automated/human review verdicts on EO artifacts (currently briefings). One row per review of a resource. |
| stg_airbyte_source__gp_api_db_campaign | raw data load from gp_api_db postgres db from table `campaign` |
| stg_airbyte_source__gp_api_db_campaign_position | raw data load from gp_api_db postgres db from table `campaign_position` |
| stg_airbyte_source__gp_api_db_chat_conversation | Chat conversations collected from EO Meeting Briefings. One row per conversation. |
| stg_airbyte_source__gp_api_db_chat_message | raw data load from gp_api_db postgres db from table `chat_message` |
| stg_airbyte_source__gp_api_db_community_issue | Community issues surfaced for an organization, ranked and categorized. One row per issue. |
| stg_airbyte_source__gp_api_db_domain | raw data load from gp_api_db postgres db from table `domain` |
| stg_airbyte_source__gp_api_db_ecanvasser | Staging model for eCanvasser stream - contains eCanvasser configuration and sync information |
| stg_airbyte_source__gp_api_db_ecanvasser_contact | Staging model for eCanvasser Contact stream - contains voter contact information from eCanvasser |
| stg_airbyte_source__gp_api_db_ecanvasser_house | Staging model for eCanvasser House stream - contains household information from eCanvasser |
| stg_airbyte_source__gp_api_db_ecanvasser_interaction | Staging model for eCanvasser Interaction stream - contains interaction records between contacts and canvassers |
| stg_airbyte_source__gp_api_db_elected_office | raw data load from gp_api_db postgres db from table `elected_office` |
| stg_airbyte_source__gp_api_db_election_type | raw data load from gp_api_db postgres db from table `election_type` |
| stg_airbyte_source__gp_api_db_experiment_run | Experiment runs from the gp-api experiment pipeline. Each row is one run of an experiment (e.g. meeting briefing/schedule generation, opposition research) for an organization, with its status, cost, and artifact pointers. One row per run_id. |
| stg_airbyte_source__gp_api_db_meeting_briefing | EO Meeting Briefing artifacts generated for an elected office and meeting date. One row per briefing. |
| stg_airbyte_source__gp_api_db_organization | raw data load from gp_api_db postgres db from table `organization` |
| stg_airbyte_source__gp_api_db_outreach | Staging model for Outreach stream - contains outreach campaign information and configuration |
| stg_airbyte_source__gp_api_db_path_to_victory | raw data load from gp_api_db postgres db from table `path_to_victory` |
| stg_airbyte_source__gp_api_db_poll | raw data load from gp_api_db postgres db from table `poll` |
| stg_airbyte_source__gp_api_db_poll_individual_message | raw data load from gp_api_db postgres db from table `poll_individual_message` |
| stg_airbyte_source__gp_api_db_poll_issues | raw data load from gp_api_db postgres db from table `poll_issues` |
| stg_airbyte_source__gp_api_db_position | raw data load from gp_api_db postgres db from table `position` |
| stg_airbyte_source__gp_api_db_race_opponent_contrast | AI-generated candidate/opponent contrast statements for a race. One row per contrast, tied to a campaign and (optionally) an opponent research finding. |
| stg_airbyte_source__gp_api_db_race_opponent_research | Opponent research runs for a race. One row per research request, tracking the run status and the opponent being researched. |
| stg_airbyte_source__gp_api_db_race_opponent_standout_action | AI-generated standout actions attributed to an opponent. One row per standout action, tied to a campaign and an opponent. |
| stg_airbyte_source__gp_api_db_race_opponent_summary | AI-generated opponent research summaries for a race. One row per summary, tied to a campaign and an opponent. |
| stg_airbyte_source__gp_api_db_tcr_compliance | raw data load from gp_api_db postgres db from table `tcr_compliance` |
| stg_airbyte_source__gp_api_db_top_issue | raw data load from gp_api_db postgres db from table `top_issue` |
| stg_airbyte_source__gp_api_db_user | raw data load from gp_api_db postgres db from table `user` |
| stg_airbyte_source__gp_api_db_website | raw data load from gp_api_db postgres db from table `website` |
| stg_airbyte_source__gp_api_db_website_contact | raw data load from gp_api_db postgres db from table `website_contact` |
| stg_airbyte_source__gp_api_db_website_view | raw data load from gp_api_db postgres db from table `website_view` |
| stg_airbyte_source__hubspot_api_companies | Companies data load from HubSpot API |
| stg_airbyte_source__hubspot_api_contacts | Contacts data loaded from the HubSpot API. Most columns are native HubSpot Contact properties. A few columns are denormalized snapshots of data that is authoritative elsewhere in this dbt project — they are kept here for convenience/auditing but downstream models should prefer the authoritative source: - `br_candidacy_id`, `br_position_id`, `br_race_id`: BallotReady IDs. Authoritative source is the BallotReady staging/intermediate models. - `goodparty_user_id`: mirrors `gp_api_db_user.id`. Authoritative source is the users staging model from the gp_api database. - `win_icp`, `serve_icp`: HubSpot holds a snapshot of our ICP model output. Authoritative ICP comes from the ICP dbt model. - `win_activated_user`, `serve_activated_user`: mirror activation state from the product users table. |
| stg_airbyte_source__hubspot_api_deals | Deals data load from HubSpot API |
| stg_airbyte_source__hubspot_api_engagements | Engagements data load from HubSpot API |
| stg_airbyte_source__hubspot_api_engagements_calls | Staged HubSpot Call engagements. One row per call activity logged in HubSpot. Sourced from the Airbyte HubSpot connector's "Engagements Calls" stream. Airbyte stores HubSpot properties as flat `properties_*`-prefixed columns; this staging model strips that prefix so downstream queries use the names from the HubSpot calls API docs. Use this for connect-rate analysis on the win/serve funnel — calls associate to contacts via the `contacts` JSON array column (lateral-flatten and join to `stg_airbyte_source__hubspot_api_contacts.id`); the `hs_call_callee_object_*` fields are sparsely populated and cover only ~2.6% of calls. |
| stg_airbyte_source__hubspot_api_feedback_submissions | Staged HubSpot Feedback Survey submissions. Contains responses from all feedback surveys including PMF, user satisfaction, and research compensation surveys. One row per submission. |
| stg_airbyte_source__hubspot_api_owners | Owners data load from HubSpot API |
| stg_airbyte_source__hubspot_api_owners_archived | Archived owners data load from HubSpot API |
| stg_airbyte_source__hubspot_api_tickets | Tickets data load from HubSpot API |
| stg_airbyte_source__stripe_api_accounts | Staging model for Stripe Accounts stream - contains account information and settings with Full Refresh sync |
| stg_airbyte_source__stripe_api_application_fees | Staging model for Stripe Application Fees stream - contains application fees charged on transactions with Incremental sync |
| stg_airbyte_source__stripe_api_application_fees_refunds | Staging model for Stripe Application Fee Refunds stream - contains refunded application fees with Incremental sync |
| stg_airbyte_source__stripe_api_balance_transactions | Staging model for Stripe Balance Transactions stream - contains account balance movements with Incremental sync |
| stg_airbyte_source__stripe_api_charges | Staging model for Stripe Charges stream - contains payment charge transactions with Incremental sync |
| stg_airbyte_source__stripe_api_customer_balance_transactions | Staging model for Stripe Customer Balance Transactions stream - contains customer account balance changes with Incremental sync |
| stg_airbyte_source__stripe_api_customers | Staging model for Stripe Customers stream - contains customer records and account information with Incremental sync |
| stg_airbyte_source__stripe_api_events | Staging model for Stripe Events stream - contains webhook events and API activity logs with Incremental sync. Note: Access guaranteed only for last 30 days |
| stg_airbyte_source__stripe_api_invoice_items | Staging model for Stripe Invoice Items stream - contains invoice line items and billing details with Incremental sync |
| stg_airbyte_source__stripe_api_invoice_line_items | Staging model for Stripe Invoice Line Items stream - contains detailed billing breakdowns with Full Refresh sync |
| stg_airbyte_source__stripe_api_invoices | Staging model for Stripe Invoices stream - contains invoice records and billing information with Incremental sync |
| stg_airbyte_source__stripe_api_payouts | Staging model for Stripe Payouts stream - contains payout transactions to connected accounts with Incremental sync |
| stg_airbyte_source__stripe_api_persons | Staging model for Stripe Persons stream - contains person records for connected accounts with Incremental sync |
| stg_airbyte_source__stripe_api_plans | Staging model for Stripe Plans stream - contains subscription plans and pricing tiers with Incremental sync |
| stg_airbyte_source__stripe_api_prices | Staging model for Stripe Prices stream - contains pricing information for products with Incremental sync |
| stg_airbyte_source__stripe_api_products | Staging model for Stripe Products stream - contains product catalog and inventory information with Incremental sync |
| stg_airbyte_source__stripe_api_promotion_codes | Staging model for Stripe Promotion Codes stream - contains promotion codes and discount campaigns with Incremental sync |
| stg_airbyte_source__stripe_api_refunds | Staging model for Stripe Refunds stream - contains refund transactions and processing details with Incremental sync |
| stg_airbyte_source__stripe_api_subscription_items | Staging model for Stripe Subscription Items stream - contains subscription line items and plan details with Full Refresh sync |
| stg_airbyte_source__stripe_api_subscription_schedule | Staging model for Stripe Subscription Schedule stream - contains subscription scheduling and recurring payment plans with Incremental sync |
| stg_airbyte_source__stripe_api_subscriptions | Staging model for Stripe Subscriptions stream - contains subscription records and recurring billing information with Incremental sync |
| stg_airbyte_source__stripe_api_transactions | Staging model for Stripe Transactions stream - contains financial transactions and payment processing records with Incremental sync |
| stg_airbyte_source__stripe_api_transfer_reversals | Staging model for Stripe Transfer Reversals stream - contains transfer reversal transactions and dispute handling with Full Refresh sync |
| stg_airbyte_source__stripe_api_transfers | Staging model for Stripe Transfers stream - contains transfer transactions between accounts with Incremental sync |
| stg_airbyte_source__techspeed_gdrive_candidates | Candidates data from TechSpeed Google Drive. Renames source columns, cleans phone numbers, normalizes date separators (slash to dash), trims text fields, and casts boolean columns via cast_to_boolean. Invalid rows (null name/state) are filtered via anti-join to the _invalid model. |
| stg_airbyte_source__techspeed_gdrive_candidates_invalid | TechSpeed candidate rows that fail data quality checks. Currently catches rows with null/empty first_name, last_name, or state. |
| stg_airbyte_source__techspeed_gdrive_marketing_data_enrichment | Marketing data enrichment load from TechSpeed Google Drive |
| stg_airbyte_source__techspeed_gdrive_officeholders | Officeholders data load from TechSpeed Google Drive |
| stg_airflow_source__l2_expired_voters | Staged view of expired L2 voter IDs ingested by the Airflow l2_expired_voters DAG. Each row is a LALVOTERID that was flagged as expired on L2's SFTP server. |
| stg_airflow_source__l2_expired_voters_loads | Load metadata for the l2_expired_voters DAG. Each row represents a successful DAG run where all batch inserts completed. Used by the DAG for idempotency — only files with a record here are considered fully loaded. |
| stg_census__block_population | 2020 decennial census population per census block, unioned from the two research-staged NHGIS extracts. The file split is by row count, not state, so both tables are required for a complete national frame (Missouri spans both). Covers all states, DC, and Puerto Rico. The 2020 frame is static, so singular tests pin the exact national totals and Missouri's block count as union regressions. |
| stg_dbt_source__l2_s3_ak_demographic | AK (Alaska) Demographic data |
| stg_dbt_source__l2_s3_ak_demographic_data_dictionary | Staging model for AK (Alaska) demographic data dictionary |
| stg_dbt_source__l2_s3_ak_haystaq_dna_flags | Staging model for AK Haystaq DNA flags |
| stg_dbt_source__l2_s3_ak_haystaq_dna_scores | Staging model for AK Haystaq DNA scores |
| stg_dbt_source__l2_s3_ak_uniform | Staging model for AK (Alaska) uniform |
| stg_dbt_source__l2_s3_ak_uniform_data_dictionary | Staging model for AK (Alaska) uniform data dictionary |
| stg_dbt_source__l2_s3_ak_vote_history | Staging model for AK (Alaska) vote history |
| stg_dbt_source__l2_s3_ak_vote_history_data_dictionary | Staging model for AK (Alaska) vote history data dictionary |
| stg_dbt_source__l2_s3_al_demographic | Staging model for AL (Alabama) demographic |
| stg_dbt_source__l2_s3_al_demographic_data_dictionary | Staging model for AL (Alabama) demographic data dictionary |
| stg_dbt_source__l2_s3_al_haystaq_dna_flags | Staging model for AL Haystaq DNA flags |
| stg_dbt_source__l2_s3_al_haystaq_dna_scores | Staging model for AL Haystaq DNA scores |
| stg_dbt_source__l2_s3_al_uniform | Staging model for AL (Alabama) uniform |
| stg_dbt_source__l2_s3_al_uniform_data_dictionary | Staging model for AL (Alabama) uniform data dictionary |
| stg_dbt_source__l2_s3_al_vote_history | Staging model for AL (Alabama) vote history |
| stg_dbt_source__l2_s3_al_vote_history_data_dictionary | Staging model for AL (Alabama) vote history data dictionary |
| stg_dbt_source__l2_s3_ar_demographic | Staging model for AR (Arkansas) demographic |
| stg_dbt_source__l2_s3_ar_demographic_data_dictionary | Staging model for AR (Arkansas) demographic data dictionary |
| stg_dbt_source__l2_s3_ar_haystaq_dna_flags | Staging model for AR Haystaq DNA flags |
| stg_dbt_source__l2_s3_ar_haystaq_dna_scores | Staging model for AR Haystaq DNA scores |
| stg_dbt_source__l2_s3_ar_uniform | Staging model for AR (Arkansas) uniform |
| stg_dbt_source__l2_s3_ar_uniform_data_dictionary | Staging model for AR (Arkansas) uniform data dictionary |
| stg_dbt_source__l2_s3_ar_vote_history | Staging model for AR (Arkansas) vote history |
| stg_dbt_source__l2_s3_ar_vote_history_data_dictionary | Staging model for AR (Arkansas) vote history data dictionary |
| stg_dbt_source__l2_s3_az_demographic | Staging model for AZ (Arizona) demographic |
| stg_dbt_source__l2_s3_az_demographic_data_dictionary | Staging model for AZ (Arizona) demographic data dictionary |
| stg_dbt_source__l2_s3_az_haystaq_dna_flags | Staging model for AZ Haystaq DNA flags |
| stg_dbt_source__l2_s3_az_haystaq_dna_scores | Staging model for AZ Haystaq DNA scores |
| stg_dbt_source__l2_s3_az_uniform | Staging model for AZ (Arizona) uniform |
| stg_dbt_source__l2_s3_az_uniform_data_dictionary | Staging model for AZ (Arizona) uniform data dictionary |
| stg_dbt_source__l2_s3_az_vote_history | Staging model for AZ (Arizona) vote history |
| stg_dbt_source__l2_s3_az_vote_history_data_dictionary | Staging model for AZ (Arizona) vote history data dictionary |
| stg_dbt_source__l2_s3_ca_demographic | Staging model for CA (California) demographic |
| stg_dbt_source__l2_s3_ca_demographic_data_dictionary | Staging model for CA (California) demographic data dictionary |
| stg_dbt_source__l2_s3_ca_haystaq_dna_flags | Staging model for CA Haystaq DNA flags |
| stg_dbt_source__l2_s3_ca_haystaq_dna_scores | Staging model for CA Haystaq DNA scores |
| stg_dbt_source__l2_s3_ca_uniform | Staging model for CA (California) uniform |
| stg_dbt_source__l2_s3_ca_uniform_data_dictionary | Staging model for CA (California) uniform data dictionary |
| stg_dbt_source__l2_s3_ca_vote_history | Staging model for CA (California) vote history |
| stg_dbt_source__l2_s3_ca_vote_history_data_dictionary | Staging model for CA (California) vote history data dictionary |
| stg_dbt_source__l2_s3_co_demographic | Staging model for CO (Colorado) demographic |
| stg_dbt_source__l2_s3_co_demographic_data_dictionary | Staging model for CO (Colorado) demographic data dictionary |
| stg_dbt_source__l2_s3_co_haystaq_dna_flags | Staging model for CO Haystaq DNA flags |
| stg_dbt_source__l2_s3_co_haystaq_dna_scores | Staging model for CO Haystaq DNA scores |
| stg_dbt_source__l2_s3_co_uniform | Staging model for CO (Colorado) uniform |
| stg_dbt_source__l2_s3_co_uniform_data_dictionary | Staging model for CO (Colorado) uniform data dictionary |
| stg_dbt_source__l2_s3_co_vote_history | Staging model for CO (Colorado) vote history |
| stg_dbt_source__l2_s3_co_vote_history_data_dictionary | Staging model for CO (Colorado) vote history data dictionary |
| stg_dbt_source__l2_s3_ct_demographic | Staging model for CT (Connecticut) demographic |
| stg_dbt_source__l2_s3_ct_demographic_data_dictionary | Staging model for CT (Connecticut) demographic data dictionary |
| stg_dbt_source__l2_s3_ct_haystaq_dna_flags | Staging model for CT Haystaq DNA flags |
| stg_dbt_source__l2_s3_ct_haystaq_dna_scores | Staging model for CT Haystaq DNA scores |
| stg_dbt_source__l2_s3_ct_uniform | Staging model for CT (Connecticut) uniform |
| stg_dbt_source__l2_s3_ct_uniform_data_dictionary | Staging model for CT (Connecticut) uniform data dictionary |
| stg_dbt_source__l2_s3_ct_vote_history | Staging model for CT (Connecticut) vote history |
| stg_dbt_source__l2_s3_ct_vote_history_data_dictionary | Staging model for CT (Connecticut) vote history data dictionary |
| stg_dbt_source__l2_s3_dc_demographic | Staging model for DC (District of Columbia) demographic |
| stg_dbt_source__l2_s3_dc_demographic_data_dictionary | Staging model for DC (District of Columbia) demographic data dictionary |
| stg_dbt_source__l2_s3_dc_haystaq_dna_flags | Staging model for DC Haystaq DNA flags |
| stg_dbt_source__l2_s3_dc_haystaq_dna_scores | Staging model for DC Haystaq DNA scores |
| stg_dbt_source__l2_s3_dc_uniform | Staging model for DC (District of Columbia) uniform |
| stg_dbt_source__l2_s3_dc_uniform_data_dictionary | Staging model for DC (District of Columbia) uniform data dictionary |
| stg_dbt_source__l2_s3_dc_vote_history | Staging model for DC (District of Columbia) vote history |
| stg_dbt_source__l2_s3_dc_vote_history_data_dictionary | Staging model for DC (District of Columbia) vote history data dictionary |
| stg_dbt_source__l2_s3_de_demographic | Staging model for DE (Delaware) demographic |
| stg_dbt_source__l2_s3_de_demographic_data_dictionary | Staging model for DE (Delaware) demographic data dictionary |
| stg_dbt_source__l2_s3_de_haystaq_dna_flags | Staging model for DE Haystaq DNA flags |
| stg_dbt_source__l2_s3_de_haystaq_dna_scores | Staging model for DE Haystaq DNA scores |
| stg_dbt_source__l2_s3_de_uniform | Staging model for DE (Delaware) uniform |
| stg_dbt_source__l2_s3_de_uniform_data_dictionary | Staging model for DE (Delaware) uniform data dictionary |
| stg_dbt_source__l2_s3_de_vote_history | Staging model for DE (Delaware) vote history |
| stg_dbt_source__l2_s3_de_vote_history_data_dictionary | Staging model for DE (Delaware) vote history data dictionary |
| stg_dbt_source__l2_s3_fl_demographic | Staging model for FL (Florida) demographic |
| stg_dbt_source__l2_s3_fl_demographic_data_dictionary | Staging model for FL (Florida) demographic data dictionary |
| stg_dbt_source__l2_s3_fl_haystaq_dna_flags | Staging model for FL Haystaq DNA flags |
| stg_dbt_source__l2_s3_fl_haystaq_dna_scores | Staging model for FL Haystaq DNA scores |
| stg_dbt_source__l2_s3_fl_uniform | Staging model for FL (Florida) uniform |
| stg_dbt_source__l2_s3_fl_uniform_data_dictionary | Staging model for FL (Florida) uniform data dictionary |
| stg_dbt_source__l2_s3_fl_vote_history | Staging model for FL (Florida) vote history |
| stg_dbt_source__l2_s3_fl_vote_history_data_dictionary | Staging model for FL (Florida) vote history data dictionary |
| stg_dbt_source__l2_s3_ga_demographic | Staging model for GA (Georgia) demographic |
| stg_dbt_source__l2_s3_ga_demographic_data_dictionary | Staging model for GA (Georgia) demographic data dictionary |
| stg_dbt_source__l2_s3_ga_haystaq_dna_flags | Staging model for GA Haystaq DNA flags |
| stg_dbt_source__l2_s3_ga_haystaq_dna_scores | Staging model for GA Haystaq DNA scores |
| stg_dbt_source__l2_s3_ga_uniform | Staging model for GA (Georgia) uniform |
| stg_dbt_source__l2_s3_ga_uniform_data_dictionary | Staging model for GA (Georgia) uniform data dictionary |
| stg_dbt_source__l2_s3_ga_vote_history | Staging model for GA (Georgia) vote history |
| stg_dbt_source__l2_s3_ga_vote_history_data_dictionary | Staging model for GA (Georgia) vote history data dictionary |
| stg_dbt_source__l2_s3_hi_demographic | Staging model for HI (Hawaii) demographic |
| stg_dbt_source__l2_s3_hi_demographic_data_dictionary | Staging model for HI (Hawaii) demographic data dictionary |
| stg_dbt_source__l2_s3_hi_haystaq_dna_flags | Staging model for HI Haystaq DNA flags |
| stg_dbt_source__l2_s3_hi_haystaq_dna_scores | Staging model for HI Haystaq DNA scores |
| stg_dbt_source__l2_s3_hi_uniform | Staging model for HI (Hawaii) uniform |
| stg_dbt_source__l2_s3_hi_uniform_data_dictionary | Staging model for HI (Hawaii) uniform data dictionary |
| stg_dbt_source__l2_s3_hi_vote_history | Staging model for HI (Hawaii) vote history |
| stg_dbt_source__l2_s3_hi_vote_history_data_dictionary | Staging model for HI (Hawaii) vote history data dictionary |
| stg_dbt_source__l2_s3_ia_demographic | Staging model for IA (Iowa) demographic |
| stg_dbt_source__l2_s3_ia_demographic_data_dictionary | Staging model for IA (Iowa) demographic data dictionary |
| stg_dbt_source__l2_s3_ia_haystaq_dna_flags | Staging model for IA Haystaq DNA flags |
| stg_dbt_source__l2_s3_ia_haystaq_dna_scores | Staging model for IA Haystaq DNA scores |
| stg_dbt_source__l2_s3_ia_uniform | Staging model for IA (Iowa) uniform |
| stg_dbt_source__l2_s3_ia_uniform_data_dictionary | Staging model for IA (Iowa) uniform data dictionary |
| stg_dbt_source__l2_s3_ia_vote_history | Staging model for IA (Iowa) vote history |
| stg_dbt_source__l2_s3_ia_vote_history_data_dictionary | Staging model for IA (Iowa) vote history data dictionary |
| stg_dbt_source__l2_s3_id_demographic | Staging model for ID (Idaho) demographic |
| stg_dbt_source__l2_s3_id_demographic_data_dictionary | Staging model for ID (Idaho) demographic data dictionary |
| stg_dbt_source__l2_s3_id_haystaq_dna_flags | Staging model for ID Haystaq DNA flags |
| stg_dbt_source__l2_s3_id_haystaq_dna_scores | Staging model for ID Haystaq DNA scores |
| stg_dbt_source__l2_s3_id_uniform | Staging model for ID (Idaho) uniform |
| stg_dbt_source__l2_s3_id_uniform_data_dictionary | Staging model for ID (Idaho) uniform data dictionary |
| stg_dbt_source__l2_s3_id_vote_history | Staging model for ID (Idaho) vote history |
| stg_dbt_source__l2_s3_id_vote_history_data_dictionary | Staging model for ID (Idaho) vote history data dictionary |
| stg_dbt_source__l2_s3_il_demographic | Staging model for IL (Illinois) demographic |
| stg_dbt_source__l2_s3_il_demographic_data_dictionary | Staging model for IL (Illinois) demographic data dictionary |
| stg_dbt_source__l2_s3_il_haystaq_dna_flags | Staging model for IL Haystaq DNA flags |
| stg_dbt_source__l2_s3_il_haystaq_dna_scores | Staging model for IL Haystaq DNA scores |
| stg_dbt_source__l2_s3_il_uniform | Staging model for IL (Illinois) uniform |
| stg_dbt_source__l2_s3_il_uniform_data_dictionary | Staging model for IL (Illinois) uniform data dictionary |
| stg_dbt_source__l2_s3_il_vote_history | Staging model for IL (Illinois) vote history |
| stg_dbt_source__l2_s3_il_vote_history_data_dictionary | Staging model for IL (Illinois) vote history data dictionary |
| stg_dbt_source__l2_s3_in_demographic | Staging model for IN (Indiana) demographic |
| stg_dbt_source__l2_s3_in_demographic_data_dictionary | Staging model for IN (Indiana) demographic data dictionary |
| stg_dbt_source__l2_s3_in_haystaq_dna_flags | Staging model for IN Haystaq DNA flags |
| stg_dbt_source__l2_s3_in_haystaq_dna_scores | Staging model for IN Haystaq DNA scores |
| stg_dbt_source__l2_s3_in_uniform | Staging model for IN (Indiana) uniform |
| stg_dbt_source__l2_s3_in_uniform_data_dictionary | Staging model for IN (Indiana) uniform data dictionary |
| stg_dbt_source__l2_s3_in_vote_history | Staging model for IN (Indiana) vote history |
| stg_dbt_source__l2_s3_in_vote_history_data_dictionary | Staging model for IN (Indiana) vote history data dictionary |
| stg_dbt_source__l2_s3_ks_demographic | Staging model for KS (Kansas) demographic |
| stg_dbt_source__l2_s3_ks_demographic_data_dictionary | Staging model for KS (Kansas) demographic data dictionary |
| stg_dbt_source__l2_s3_ks_haystaq_dna_flags | Staging model for KS Haystaq DNA flags |
| stg_dbt_source__l2_s3_ks_haystaq_dna_scores | Staging model for KS Haystaq DNA scores |
| stg_dbt_source__l2_s3_ks_uniform | Staging model for KS (Kansas) uniform |
| stg_dbt_source__l2_s3_ks_uniform_data_dictionary | Staging model for KS (Kansas) uniform data dictionary |
| stg_dbt_source__l2_s3_ks_vote_history | Staging model for KS (Kansas) vote history |
| stg_dbt_source__l2_s3_ks_vote_history_data_dictionary | Staging model for KS (Kansas) vote history data dictionary |
| stg_dbt_source__l2_s3_ky_demographic | Staging model for KY (Kentucky) demographic |
| stg_dbt_source__l2_s3_ky_demographic_data_dictionary | Staging model for KY (Kentucky) demographic data dictionary |
| stg_dbt_source__l2_s3_ky_haystaq_dna_flags | Staging model for KY Haystaq DNA flags |
| stg_dbt_source__l2_s3_ky_haystaq_dna_scores | Staging model for KY Haystaq DNA scores |
| stg_dbt_source__l2_s3_ky_uniform | Staging model for KY (Kentucky) uniform |
| stg_dbt_source__l2_s3_ky_uniform_data_dictionary | Staging model for KY (Kentucky) uniform data dictionary |
| stg_dbt_source__l2_s3_ky_vote_history | Staging model for KY (Kentucky) vote history |
| stg_dbt_source__l2_s3_ky_vote_history_data_dictionary | Staging model for KY (Kentucky) vote history data dictionary |
| stg_dbt_source__l2_s3_la_demographic | Staging model for LA (Louisiana) demographic |
| stg_dbt_source__l2_s3_la_demographic_data_dictionary | Staging model for LA (Louisiana) demographic data dictionary |
| stg_dbt_source__l2_s3_la_haystaq_dna_flags | Staging model for LA Haystaq DNA flags |
| stg_dbt_source__l2_s3_la_haystaq_dna_scores | Staging model for LA Haystaq DNA scores |
| stg_dbt_source__l2_s3_la_uniform | Staging model for LA (Louisiana) uniform |
| stg_dbt_source__l2_s3_la_uniform_data_dictionary | Staging model for LA (Louisiana) uniform data dictionary |
| stg_dbt_source__l2_s3_la_vote_history | Staging model for LA (Louisiana) vote history |
| stg_dbt_source__l2_s3_la_vote_history_data_dictionary | Staging model for LA (Louisiana) vote history data dictionary |
| stg_dbt_source__l2_s3_ma_demographic | Staging model for MA (Massachusetts) demographic |
| stg_dbt_source__l2_s3_ma_demographic_data_dictionary | Staging model for MA (Massachusetts) demographic data dictionary |
| stg_dbt_source__l2_s3_ma_haystaq_dna_flags | Staging model for MA Haystaq DNA flags |
| stg_dbt_source__l2_s3_ma_haystaq_dna_scores | Staging model for MA Haystaq DNA scores |
| stg_dbt_source__l2_s3_ma_uniform | Staging model for MA (Massachusetts) uniform |
| stg_dbt_source__l2_s3_ma_uniform_data_dictionary | Staging model for MA (Massachusetts) uniform data dictionary |
| stg_dbt_source__l2_s3_ma_vote_history | Staging model for MA (Massachusetts) vote history |
| stg_dbt_source__l2_s3_ma_vote_history_data_dictionary | Staging model for MA (Massachusetts) vote history data dictionary |
| stg_dbt_source__l2_s3_md_demographic | Staging model for MD (Maryland) demographic |
| stg_dbt_source__l2_s3_md_demographic_data_dictionary | Staging model for MD (Maryland) demographic data dictionary |
| stg_dbt_source__l2_s3_md_haystaq_dna_flags | Staging model for MD Haystaq DNA flags |
| stg_dbt_source__l2_s3_md_haystaq_dna_scores | Staging model for MD Haystaq DNA scores |
| stg_dbt_source__l2_s3_md_uniform | Staging model for MD (Maryland) uniform |
| stg_dbt_source__l2_s3_md_uniform_data_dictionary | Staging model for MD (Maryland) uniform data dictionary |
| stg_dbt_source__l2_s3_md_vote_history | Staging model for MD (Maryland) vote history |
| stg_dbt_source__l2_s3_md_vote_history_data_dictionary | Staging model for MD (Maryland) vote history data dictionary |
| stg_dbt_source__l2_s3_me_demographic | Staging model for ME (Maine) demographic |
| stg_dbt_source__l2_s3_me_demographic_data_dictionary | Staging model for ME (Maine) demographic data dictionary |
| stg_dbt_source__l2_s3_me_haystaq_dna_flags | Staging model for ME Haystaq DNA flags |
| stg_dbt_source__l2_s3_me_haystaq_dna_scores | Staging model for ME Haystaq DNA scores |
| stg_dbt_source__l2_s3_me_uniform | Staging model for ME (Maine) uniform |
| stg_dbt_source__l2_s3_me_uniform_data_dictionary | Staging model for ME (Maine) uniform data dictionary |
| stg_dbt_source__l2_s3_me_vote_history | Staging model for ME (Maine) vote history |
| stg_dbt_source__l2_s3_me_vote_history_data_dictionary | Staging model for ME (Maine) vote history data dictionary |
| stg_dbt_source__l2_s3_mi_demographic | Staging model for MI (Michigan) demographic |
| stg_dbt_source__l2_s3_mi_demographic_data_dictionary | Staging model for MI (Michigan) demographic data dictionary |
| stg_dbt_source__l2_s3_mi_haystaq_dna_flags | Staging model for MI Haystaq DNA flags |
| stg_dbt_source__l2_s3_mi_haystaq_dna_scores | Staging model for MI Haystaq DNA scores |
| stg_dbt_source__l2_s3_mi_uniform | Staging model for MI (Michigan) uniform |
| stg_dbt_source__l2_s3_mi_uniform_data_dictionary | Staging model for MI (Michigan) uniform data dictionary |
| stg_dbt_source__l2_s3_mi_vote_history | Staging model for MI (Michigan) vote history |
| stg_dbt_source__l2_s3_mi_vote_history_data_dictionary | Staging model for MI (Michigan) vote history data dictionary |
| stg_dbt_source__l2_s3_mn_demographic | Staging model for MN (Minnesota) demographic |
| stg_dbt_source__l2_s3_mn_demographic_data_dictionary | Staging model for MN (Minnesota) demographic data dictionary |
| stg_dbt_source__l2_s3_mn_haystaq_dna_flags | Staging model for MN Haystaq DNA flags |
| stg_dbt_source__l2_s3_mn_haystaq_dna_scores | Staging model for MN Haystaq DNA scores |
| stg_dbt_source__l2_s3_mn_uniform | Staging model for MN (Minnesota) uniform |
| stg_dbt_source__l2_s3_mn_uniform_data_dictionary | Staging model for MN (Minnesota) uniform data dictionary |
| stg_dbt_source__l2_s3_mn_vote_history | Staging model for MN (Minnesota) vote history |
| stg_dbt_source__l2_s3_mn_vote_history_data_dictionary | Staging model for MN (Minnesota) vote history data dictionary |
| stg_dbt_source__l2_s3_mo_demographic | Staging model for MO (Missouri) demographic |
| stg_dbt_source__l2_s3_mo_demographic_data_dictionary | Staging model for MO (Missouri) demographic data dictionary |
| stg_dbt_source__l2_s3_mo_haystaq_dna_flags | Staging model for MO Haystaq DNA flags |
| stg_dbt_source__l2_s3_mo_haystaq_dna_scores | Staging model for MO Haystaq DNA scores |
| stg_dbt_source__l2_s3_mo_uniform | Staging model for MO (Missouri) uniform |
| stg_dbt_source__l2_s3_mo_uniform_data_dictionary | Staging model for MO (Missouri) uniform data dictionary |
| stg_dbt_source__l2_s3_mo_vote_history | Staging model for MO (Missouri) vote history |
| stg_dbt_source__l2_s3_mo_vote_history_data_dictionary | Staging model for MO (Missouri) vote history data dictionary |
| stg_dbt_source__l2_s3_ms_demographic | Staging model for MS (Mississippi) demographic |
| stg_dbt_source__l2_s3_ms_demographic_data_dictionary | Staging model for MS (Mississippi) demographic data dictionary |
| stg_dbt_source__l2_s3_ms_haystaq_dna_flags | Staging model for MS Haystaq DNA flags |
| stg_dbt_source__l2_s3_ms_haystaq_dna_scores | Staging model for MS Haystaq DNA scores |
| stg_dbt_source__l2_s3_ms_uniform | Staging model for MS (Mississippi) uniform |
| stg_dbt_source__l2_s3_ms_uniform_data_dictionary | Staging model for MS (Mississippi) uniform data dictionary |
| stg_dbt_source__l2_s3_ms_vote_history | Staging model for MS (Mississippi) vote history |
| stg_dbt_source__l2_s3_ms_vote_history_data_dictionary | Staging model for MS (Mississippi) vote history data dictionary |
| stg_dbt_source__l2_s3_mt_demographic | Staging model for MT (Montana) demographic |
| stg_dbt_source__l2_s3_mt_demographic_data_dictionary | Staging model for MT (Montana) demographic data dictionary |
| stg_dbt_source__l2_s3_mt_haystaq_dna_flags | Staging model for MT Haystaq DNA flags |
| stg_dbt_source__l2_s3_mt_haystaq_dna_scores | Staging model for MT Haystaq DNA scores |
| stg_dbt_source__l2_s3_mt_uniform | Staging model for MT (Montana) uniform |
| stg_dbt_source__l2_s3_mt_uniform_data_dictionary | Staging model for MT (Montana) uniform data dictionary |
| stg_dbt_source__l2_s3_mt_vote_history | Staging model for MT (Montana) vote history |
| stg_dbt_source__l2_s3_mt_vote_history_data_dictionary | Staging model for MT (Montana) vote history data dictionary |
| stg_dbt_source__l2_s3_nc_demographic | Staging model for NC (North Carolina) demographic |
| stg_dbt_source__l2_s3_nc_demographic_data_dictionary | Staging model for NC (North Carolina) demographic data dictionary |
| stg_dbt_source__l2_s3_nc_haystaq_dna_flags | Staging model for NC Haystaq DNA flags |
| stg_dbt_source__l2_s3_nc_haystaq_dna_scores | Staging model for NC Haystaq DNA scores |
| stg_dbt_source__l2_s3_nc_uniform | Staging model for NC (North Carolina) uniform |
| stg_dbt_source__l2_s3_nc_uniform_data_dictionary | Staging model for NC (North Carolina) uniform data dictionary |
| stg_dbt_source__l2_s3_nc_vote_history | Staging model for NC (North Carolina) vote history |
| stg_dbt_source__l2_s3_nc_vote_history_data_dictionary | Staging model for NC (North Carolina) vote history data dictionary |
| stg_dbt_source__l2_s3_nd_demographic | Staging model for ND (North Dakota) demographic |
| stg_dbt_source__l2_s3_nd_demographic_data_dictionary | Staging model for ND (North Dakota) demographic data dictionary |
| stg_dbt_source__l2_s3_nd_haystaq_dna_flags | Staging model for ND Haystaq DNA flags |
| stg_dbt_source__l2_s3_nd_haystaq_dna_scores | Staging model for ND Haystaq DNA scores |
| stg_dbt_source__l2_s3_nd_uniform | Staging model for ND (North Dakota) uniform |
| stg_dbt_source__l2_s3_nd_uniform_data_dictionary | Staging model for ND (North Dakota) uniform data dictionary |
| stg_dbt_source__l2_s3_nd_vote_history | Staging model for ND (North Dakota) vote history |
| stg_dbt_source__l2_s3_nd_vote_history_data_dictionary | Staging model for ND (North Dakota) vote history data dictionary |
| stg_dbt_source__l2_s3_ne_demographic | Staging model for NE (Nebraska) demographic |
| stg_dbt_source__l2_s3_ne_demographic_data_dictionary | Staging model for NE (Nebraska) demographic data dictionary |
| stg_dbt_source__l2_s3_ne_haystaq_dna_flags | Staging model for NE Haystaq DNA flags |
| stg_dbt_source__l2_s3_ne_haystaq_dna_scores | Staging model for NE Haystaq DNA scores |
| stg_dbt_source__l2_s3_ne_uniform | Staging model for NE (Nebraska) uniform |
| stg_dbt_source__l2_s3_ne_uniform_data_dictionary | Staging model for NE (Nebraska) uniform data dictionary |
| stg_dbt_source__l2_s3_ne_vote_history | Staging model for NE (Nebraska) vote history |
| stg_dbt_source__l2_s3_ne_vote_history_data_dictionary | Staging model for NE (Nebraska) vote history data dictionary |
| stg_dbt_source__l2_s3_nh_demographic | Staging model for NH (New Hampshire) demographic |
| stg_dbt_source__l2_s3_nh_demographic_data_dictionary | Staging model for NH (New Hampshire) demographic data dictionary |
| stg_dbt_source__l2_s3_nh_haystaq_dna_flags | Staging model for NH Haystaq DNA flags |
| stg_dbt_source__l2_s3_nh_haystaq_dna_scores | Staging model for NH Haystaq DNA scores |
| stg_dbt_source__l2_s3_nh_uniform | Staging model for NH (New Hampshire) uniform |
| stg_dbt_source__l2_s3_nh_uniform_data_dictionary | Staging model for NH (New Hampshire) uniform data dictionary |
| stg_dbt_source__l2_s3_nh_vote_history | Staging model for NH (New Hampshire) vote history |
| stg_dbt_source__l2_s3_nh_vote_history_data_dictionary | Staging model for NH (New Hampshire) vote history data dictionary |
| stg_dbt_source__l2_s3_nj_demographic | Staging model for NJ (New Jersey) demographic |
| stg_dbt_source__l2_s3_nj_demographic_data_dictionary | Staging model for NJ (New Jersey) demographic data dictionary |
| stg_dbt_source__l2_s3_nj_haystaq_dna_flags | Staging model for NJ Haystaq DNA flags |
| stg_dbt_source__l2_s3_nj_haystaq_dna_scores | Staging model for NJ Haystaq DNA scores |
| stg_dbt_source__l2_s3_nj_uniform | Staging model for NJ (New Jersey) uniform |
| stg_dbt_source__l2_s3_nj_uniform_data_dictionary | Staging model for NJ (New Jersey) uniform data dictionary |
| stg_dbt_source__l2_s3_nj_vote_history | Staging model for NJ (New Jersey) vote history |
| stg_dbt_source__l2_s3_nj_vote_history_data_dictionary | Staging model for NJ (New Jersey) vote history data dictionary |
| stg_dbt_source__l2_s3_nm_demographic | Staging model for NM (New Mexico) demographic |
| stg_dbt_source__l2_s3_nm_demographic_data_dictionary | Staging model for NM (New Mexico) demographic data dictionary |
| stg_dbt_source__l2_s3_nm_haystaq_dna_flags | Staging model for NM Haystaq DNA flags |
| stg_dbt_source__l2_s3_nm_haystaq_dna_scores | Staging model for NM Haystaq DNA scores |
| stg_dbt_source__l2_s3_nm_uniform | Staging model for NM (New Mexico) uniform |
| stg_dbt_source__l2_s3_nm_uniform_data_dictionary | Staging model for NM (New Mexico) uniform data dictionary |
| stg_dbt_source__l2_s3_nm_vote_history | Staging model for NM (New Mexico) vote history |
| stg_dbt_source__l2_s3_nm_vote_history_data_dictionary | Staging model for NM (New Mexico) vote history data dictionary |
| stg_dbt_source__l2_s3_nv_demographic | Staging model for NV (Nevada) demographic |
| stg_dbt_source__l2_s3_nv_demographic_data_dictionary | Staging model for NV (Nevada) demographic data dictionary |
| stg_dbt_source__l2_s3_nv_haystaq_dna_flags | Staging model for NV Haystaq DNA flags |
| stg_dbt_source__l2_s3_nv_haystaq_dna_scores | Staging model for NV Haystaq DNA scores |
| stg_dbt_source__l2_s3_nv_uniform | Staging model for NV (Nevada) uniform |
| stg_dbt_source__l2_s3_nv_uniform_data_dictionary | Staging model for NV (Nevada) uniform data dictionary |
| stg_dbt_source__l2_s3_nv_vote_history | Staging model for NV (Nevada) vote history |
| stg_dbt_source__l2_s3_nv_vote_history_data_dictionary | Staging model for NV (Nevada) vote history data dictionary |
| stg_dbt_source__l2_s3_ny_demographic | Staging model for NY (New York) demographic |
| stg_dbt_source__l2_s3_ny_demographic_data_dictionary | Staging model for NY (New York) demographic data dictionary |
| stg_dbt_source__l2_s3_ny_haystaq_dna_flags | Staging model for NY Haystaq DNA flags |
| stg_dbt_source__l2_s3_ny_haystaq_dna_scores | Staging model for NY Haystaq DNA scores |
| stg_dbt_source__l2_s3_ny_uniform | Staging model for NY (New York) uniform |
| stg_dbt_source__l2_s3_ny_uniform_data_dictionary | Staging model for NY (New York) uniform data dictionary |
| stg_dbt_source__l2_s3_ny_vote_history | Staging model for NY (New York) vote history |
| stg_dbt_source__l2_s3_ny_vote_history_data_dictionary | Staging model for NY (New York) vote history data dictionary |
| stg_dbt_source__l2_s3_oh_demographic | Staging model for OH (Ohio) demographic |
| stg_dbt_source__l2_s3_oh_demographic_data_dictionary | Staging model for OH (Ohio) demographic data dictionary |
| stg_dbt_source__l2_s3_oh_haystaq_dna_flags | Staging model for OH Haystaq DNA flags |
| stg_dbt_source__l2_s3_oh_haystaq_dna_scores | Staging model for OH Haystaq DNA scores |
| stg_dbt_source__l2_s3_oh_uniform | Staging model for OH (Ohio) uniform |
| stg_dbt_source__l2_s3_oh_uniform_data_dictionary | Staging model for OH (Ohio) uniform data dictionary |
| stg_dbt_source__l2_s3_oh_vote_history | Staging model for OH (Ohio) vote history |
| stg_dbt_source__l2_s3_oh_vote_history_data_dictionary | Staging model for OH (Ohio) vote history data dictionary |
| stg_dbt_source__l2_s3_ok_demographic | Staging model for OK (Oklahoma) demographic |
| stg_dbt_source__l2_s3_ok_demographic_data_dictionary | Staging model for OK (Oklahoma) demographic data dictionary |
| stg_dbt_source__l2_s3_ok_haystaq_dna_flags | Staging model for OK Haystaq DNA flags |
| stg_dbt_source__l2_s3_ok_haystaq_dna_scores | Staging model for OK Haystaq DNA scores |
| stg_dbt_source__l2_s3_ok_uniform | Staging model for OK (Oklahoma) uniform |
| stg_dbt_source__l2_s3_ok_uniform_data_dictionary | Staging model for OK (Oklahoma) uniform data dictionary |
| stg_dbt_source__l2_s3_ok_vote_history | Staging model for OK (Oklahoma) vote history |
| stg_dbt_source__l2_s3_ok_vote_history_data_dictionary | Staging model for OK (Oklahoma) vote history data dictionary |
| stg_dbt_source__l2_s3_or_demographic | Staging model for OR (Oregon) demographic |
| stg_dbt_source__l2_s3_or_demographic_data_dictionary | Staging model for OR (Oregon) demographic data dictionary |
| stg_dbt_source__l2_s3_or_haystaq_dna_flags | Staging model for OR Haystaq DNA flags |
| stg_dbt_source__l2_s3_or_haystaq_dna_scores | Staging model for OR Haystaq DNA scores |
| stg_dbt_source__l2_s3_or_uniform | Staging model for OR (Oregon) uniform |
| stg_dbt_source__l2_s3_or_uniform_data_dictionary | Staging model for OR (Oregon) uniform data dictionary |
| stg_dbt_source__l2_s3_or_vote_history | Staging model for OR (Oregon) vote history |
| stg_dbt_source__l2_s3_or_vote_history_data_dictionary | Staging model for OR (Oregon) vote history data dictionary |
| stg_dbt_source__l2_s3_pa_demographic | Staging model for PA (Pennsylvania) demographic |
| stg_dbt_source__l2_s3_pa_demographic_data_dictionary | Staging model for PA (Pennsylvania) demographic data dictionary |
| stg_dbt_source__l2_s3_pa_haystaq_dna_flags | Staging model for PA Haystaq DNA flags |
| stg_dbt_source__l2_s3_pa_haystaq_dna_scores | Staging model for PA Haystaq DNA scores |
| stg_dbt_source__l2_s3_pa_uniform | Staging model for PA (Pennsylvania) uniform |
| stg_dbt_source__l2_s3_pa_uniform_data_dictionary | Staging model for PA (Pennsylvania) uniform data dictionary |
| stg_dbt_source__l2_s3_pa_vote_history | Staging model for PA (Pennsylvania) vote history |
| stg_dbt_source__l2_s3_pa_vote_history_data_dictionary | Staging model for PA (Pennsylvania) vote history data dictionary |
| stg_dbt_source__l2_s3_ri_demographic | Staging model for RI (Rhode Island) demographic |
| stg_dbt_source__l2_s3_ri_demographic_data_dictionary | Staging model for RI (Rhode Island) demographic data dictionary |
| stg_dbt_source__l2_s3_ri_haystaq_dna_flags | Staging model for RI Haystaq DNA flags |
| stg_dbt_source__l2_s3_ri_haystaq_dna_scores | Staging model for RI Haystaq DNA scores |
| stg_dbt_source__l2_s3_ri_uniform | Staging model for RI (Rhode Island) uniform |
| stg_dbt_source__l2_s3_ri_uniform_data_dictionary | Staging model for RI (Rhode Island) uniform data dictionary |
| stg_dbt_source__l2_s3_ri_vote_history | Staging model for RI (Rhode Island) vote history |
| stg_dbt_source__l2_s3_ri_vote_history_data_dictionary | Staging model for RI (Rhode Island) vote history data dictionary |
| stg_dbt_source__l2_s3_sc_demographic | Staging model for SC (South Carolina) demographic |
| stg_dbt_source__l2_s3_sc_demographic_data_dictionary | Staging model for SC (South Carolina) demographic data dictionary |
| stg_dbt_source__l2_s3_sc_haystaq_dna_flags | Staging model for SC Haystaq DNA flags |
| stg_dbt_source__l2_s3_sc_haystaq_dna_scores | Staging model for SC Haystaq DNA scores |
| stg_dbt_source__l2_s3_sc_uniform | Staging model for SC (South Carolina) uniform |
| stg_dbt_source__l2_s3_sc_uniform_data_dictionary | Staging model for SC (South Carolina) uniform data dictionary |
| stg_dbt_source__l2_s3_sc_vote_history | Staging model for SC (South Carolina) vote history |
| stg_dbt_source__l2_s3_sc_vote_history_data_dictionary | Staging model for SC (South Carolina) vote history data dictionary |
| stg_dbt_source__l2_s3_sd_demographic | Staging model for SD (South Dakota) demographic |
| stg_dbt_source__l2_s3_sd_demographic_data_dictionary | Staging model for SD (South Dakota) demographic data dictionary |
| stg_dbt_source__l2_s3_sd_haystaq_dna_flags | Staging model for SD Haystaq DNA flags |
| stg_dbt_source__l2_s3_sd_haystaq_dna_scores | Staging model for SD Haystaq DNA scores |
| stg_dbt_source__l2_s3_sd_uniform | Staging model for SD (South Dakota) uniform |
| stg_dbt_source__l2_s3_sd_uniform_data_dictionary | Staging model for SD (South Dakota) uniform data dictionary |
| stg_dbt_source__l2_s3_sd_vote_history | Staging model for SD (South Dakota) vote history |
| stg_dbt_source__l2_s3_sd_vote_history_data_dictionary | Staging model for SD (South Dakota) vote history data dictionary |
| stg_dbt_source__l2_s3_tn_demographic | Staging model for TN (Tennessee) demographic |
| stg_dbt_source__l2_s3_tn_demographic_data_dictionary | Staging model for TN (Tennessee) demographic data dictionary |
| stg_dbt_source__l2_s3_tn_haystaq_dna_flags | Staging model for TN Haystaq DNA flags |
| stg_dbt_source__l2_s3_tn_haystaq_dna_scores | Staging model for TN Haystaq DNA scores |
| stg_dbt_source__l2_s3_tn_uniform | Staging model for TN (Tennessee) uniform |
| stg_dbt_source__l2_s3_tn_uniform_data_dictionary | Staging model for TN (Tennessee) uniform data dictionary |
| stg_dbt_source__l2_s3_tn_vote_history | Staging model for TN (Tennessee) vote history |
| stg_dbt_source__l2_s3_tn_vote_history_data_dictionary | Staging model for TN (Tennessee) vote history data dictionary |
| stg_dbt_source__l2_s3_tx_demographic | Staging model for TX (Texas) demographic |
| stg_dbt_source__l2_s3_tx_demographic_data_dictionary | Staging model for TX (Texas) demographic data dictionary |
| stg_dbt_source__l2_s3_tx_haystaq_dna_flags | Staging model for TX Haystaq DNA flags |
| stg_dbt_source__l2_s3_tx_haystaq_dna_scores | Staging model for TX Haystaq DNA scores |
| stg_dbt_source__l2_s3_tx_uniform | Staging model for TX (Texas) uniform |
| stg_dbt_source__l2_s3_tx_uniform_data_dictionary | Staging model for TX (Texas) uniform data dictionary |
| stg_dbt_source__l2_s3_tx_vote_history | Staging model for TX (Texas) vote history |
| stg_dbt_source__l2_s3_tx_vote_history_data_dictionary | Staging model for TX (Texas) vote history data dictionary |
| stg_dbt_source__l2_s3_ut_demographic | taging model for UT (Utah) demographic |
| stg_dbt_source__l2_s3_ut_demographic_data_dictionary | Staging model for UT (Utah) demographic data dictionary |
| stg_dbt_source__l2_s3_ut_haystaq_dna_flags | Staging model for UT Haystaq DNA flags |
| stg_dbt_source__l2_s3_ut_haystaq_dna_scores | Staging model for UT Haystaq DNA scores |
| stg_dbt_source__l2_s3_ut_uniform | Staging model for UT (Utah) uniform |
| stg_dbt_source__l2_s3_ut_uniform_data_dictionary | Staging model for UT (Utah) uniform data dictionary |
| stg_dbt_source__l2_s3_ut_vote_history | Staging model for UT (Utah) vote history |
| stg_dbt_source__l2_s3_ut_vote_history_data_dictionary | Staging model for UT (Utah) vote history data dictionary |
| stg_dbt_source__l2_s3_va_demographic | Staging model for VA (Virginia) demographic |
| stg_dbt_source__l2_s3_va_demographic_data_dictionary | Staging model for VA (Virginia) demographic data dictionary |
| stg_dbt_source__l2_s3_va_haystaq_dna_flags | Staging model for VA Haystaq DNA flags |
| stg_dbt_source__l2_s3_va_haystaq_dna_scores | Staging model for VA Haystaq DNA scores |
| stg_dbt_source__l2_s3_va_uniform | Staging model for VA (Virginia) uniform |
| stg_dbt_source__l2_s3_va_uniform_data_dictionary | Staging model for VA (Virginia) uniform data dictionary |
| stg_dbt_source__l2_s3_va_vote_history | Staging model for VA (Virginia) vote history |
| stg_dbt_source__l2_s3_va_vote_history_data_dictionary | Staging model for VA (Virginia) vote history data dictionary |
| stg_dbt_source__l2_s3_vt_demographic | Staging model for VT (Vermont) demographic |
| stg_dbt_source__l2_s3_vt_demographic_data_dictionary | Staging model for VT (Vermont) demographic data dictionary |
| stg_dbt_source__l2_s3_vt_haystaq_dna_flags | Staging model for VT Haystaq DNA flags |
| stg_dbt_source__l2_s3_vt_haystaq_dna_scores | Staging model for VT Haystaq DNA scores |
| stg_dbt_source__l2_s3_vt_uniform | Staging model for VT (Vermont) uniform |
| stg_dbt_source__l2_s3_vt_uniform_data_dictionary | Staging model for VT (Vermont) uniform data dictionary |
| stg_dbt_source__l2_s3_vt_vote_history | Staging model for VT (Vermont) vote history |
| stg_dbt_source__l2_s3_vt_vote_history_data_dictionary | Staging model for VT (Vermont) vote history data dictionary |
| stg_dbt_source__l2_s3_wa_demographic | Staging model for WA (Washington) demographic |
| stg_dbt_source__l2_s3_wa_demographic_data_dictionary | Staging model for WA (Washington) demographic data dictionary |
| stg_dbt_source__l2_s3_wa_haystaq_dna_flags | Staging model for WA Haystaq DNA flags |
| stg_dbt_source__l2_s3_wa_haystaq_dna_scores | Staging model for WA Haystaq DNA scores |
| stg_dbt_source__l2_s3_wa_uniform | Staging model for WA (Washington) uniform |
| stg_dbt_source__l2_s3_wa_uniform_data_dictionary | Staging model for WA (Washington) uniform data dictionary |
| stg_dbt_source__l2_s3_wa_vote_history | Staging model for WA (Washington) vote history |
| stg_dbt_source__l2_s3_wa_vote_history_data_dictionary | Staging model for WA (Washington) vote history data dictionary |
| stg_dbt_source__l2_s3_wi_demographic | Staging model for WI (Wisconsin) demographic |
| stg_dbt_source__l2_s3_wi_demographic_data_dictionary | Staging model for WI (Wisconsin) demographic data dictionary |
| stg_dbt_source__l2_s3_wi_haystaq_dna_flags | Staging model for WI Haystaq DNA flags |
| stg_dbt_source__l2_s3_wi_haystaq_dna_scores | Staging model for WI Haystaq DNA scores |
| stg_dbt_source__l2_s3_wi_uniform | Staging model for WI (Wisconsin) uniform |
| stg_dbt_source__l2_s3_wi_uniform_data_dictionary | Staging model for WI (Wisconsin) uniform data dictionary |
| stg_dbt_source__l2_s3_wi_vote_history | Staging model for WI (Wisconsin) vote history |
| stg_dbt_source__l2_s3_wi_vote_history_data_dictionary | Staging model for WI (Wisconsin) vote history data dictionary |
| stg_dbt_source__l2_s3_wv_demographic | Staging model for WV (West Virginia) demographic |
| stg_dbt_source__l2_s3_wv_demographic_data_dictionary | Staging model for WV (West Virginia) demographic data dictionary |
| stg_dbt_source__l2_s3_wv_haystaq_dna_flags | Staging model for WV Haystaq DNA flags |
| stg_dbt_source__l2_s3_wv_haystaq_dna_scores | Staging model for WV Haystaq DNA scores |
| stg_dbt_source__l2_s3_wv_uniform | Staging model for WV (West Virginia) uniform |
| stg_dbt_source__l2_s3_wv_uniform_data_dictionary | Staging model for WV (West Virginia) uniform data dictionary |
| stg_dbt_source__l2_s3_wv_vote_history | Staging model for WV (West Virginia) vote history |
| stg_dbt_source__l2_s3_wv_vote_history_data_dictionary | Staging model for WV (West Virginia) vote history data dictionary |
| stg_dbt_source__l2_s3_wy_demographic | Staging model for WY (Wyoming) demographic |
| stg_dbt_source__l2_s3_wy_demographic_data_dictionary | Staging model for WY (Wyoming) demographic data dictionary |
| stg_dbt_source__l2_s3_wy_haystaq_dna_flags | Staging model for WY Haystaq DNA flags |
| stg_dbt_source__l2_s3_wy_haystaq_dna_scores | Staging model for WY Haystaq DNA scores |
| stg_dbt_source__l2_s3_wy_uniform | Staging model for WY (Wyoming) uniform |
| stg_dbt_source__l2_s3_wy_uniform_data_dictionary | Staging model for WY (Wyoming) uniform data dictionary |
| stg_dbt_source__l2_s3_wy_vote_history | Staging model for WY (Wyoming) vote history |
| stg_dbt_source__l2_s3_wy_vote_history_data_dictionary | Staging model for WY (Wyoming) vote history data dictionary |
| stg_er_source__clustered_candidacy_stages | Staged cluster assignments from the Splink entity resolution pipeline. Each row is a candidacy-stage record from BallotReady, TechSpeed, or DDHQ, annotated with a cluster_id grouping records that represent the same real-world candidacy-stage. Cross-source clusters form crosswalks between sources (e.g. BR↔TS, BR↔DDHQ). Single-record clusters are unmatched records unique to one source. The clustering uses a 0.95 match probability threshold on Splink's connected-components algorithm, after post-prediction filters enforce person identity agreement (last name + first name/email/phone), race-level agreement (office name Jaro-Winkler >= 0.75), and race ID consistency. |
| stg_er_source__clustered_elected_officials | Match groups bridging gp-api elected-office records to BR+TS terms. Term grain. Mart owns person rollup via br_candidate_id, and term selection within a multi-term cluster via sworn_in_date proximity to term_start_date. |
| stg_er_source__clustered_election_stages | Staged cluster assignments from the election-stage Splink entity resolution pipeline. Each row is a race-level record from BallotReady, DDHQ, or TechSpeed, annotated with a cluster_id grouping records that represent the same real-world election stage (race). Cross-source clusters form race-level crosswalks; single-record clusters are unmatched races unique to one source. |
| stg_er_source__pairwise_candidacy_stages | Staged pairwise predictions from the Splink entity resolution pipeline. Each row is a scored pair of candidacy-stage records with Splink match weights, match probabilities, per-comparison gamma levels (agreement categories), Bayes factors, and term-frequency adjustments. Records may come from BallotReady, TechSpeed, or DDHQ. Pairs are generated by 6 blocking rules (br_race_id exact, state + election_date + office fuzzy + last_name, state + last_name + election_date, state + election_date + office fuzzy + last_name fuzzy, phone exact, email exact) and filtered to match_probability >= 0.01. Three post-prediction filters are applied: (1) person identity — requires partial last_name match AND first_name/email/phone match, (2) race-level — requires office name Jaro-Winkler >= 0.75, (3) race ID — excludes pairs with differing integer br_race_ids unless office names match well. |
| stg_er_source__pairwise_elected_officials | Staged pairwise predictions from the elected officials entity resolution pipeline. Each row is a scored pair of elected official records with Splink match weights, match probabilities, per-comparison gamma levels, Bayes factors, and term-frequency adjustments. Pairs are generated by 5 blocking rules (state+office_fuzzy+last_name, state+office_fuzzy+last_name_fuzzy, state+last_name, phone exact, email exact) and filtered to match_probability >= 0.01. Post-prediction filters enforce person identity (last_name + first_name or contact info) and office compatibility (office_type match with contact-info bypass). No race ID filter (elected officials have no br_race_id). Compared to candidacy stages, this entity type adds office_type and ballotready_position_id as Splink comparisons and drops election_date and br_race_id. (office_level is a Splink comparison in both entity types.) |
| stg_model_predictions__ballots_projected_2026_v2 | Turnout projections for 2026 (v2 replacing original even years 2026 data) |
| stg_model_predictions__ballots_projected_2027 | Turnout projections for 2027 |
| stg_model_predictions__ballots_projected_2028 | Turnout projections for 2028 |
| stg_model_predictions__candidacy_br_matches_20251204 | fuzzy matching between BallotReady election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__candidacy_br_matches_prenov2025_20251205 | fuzzy matching between BallotReady election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__candidacy_ddhq_matches_20250826 | gemini outputs to string match DDHQ election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__candidacy_ddhq_matches_20250909 | gemini outputs to string match DDHQ election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__candidacy_ddhq_matches_20250916 | gemini outputs to string match DDHQ election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__candidacy_ddhq_matches_20251016 | gemini outputs to string match DDHQ election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__candidacy_ddhq_matches_20251112 | gemini outputs to string match DDHQ election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__candidacy_ddhq_matches_20251202 | gemini outputs to string match DDHQ election results and candidacies sourced from m_general__candidacy |
| stg_model_predictions__llm_l2_br_match_20250806 | gemini outputs to string match L2 districts to BallotReady positions/offices |
| stg_model_predictions__llm_l2_br_match_20250811 | gemini outputs to string match L2 districts to BallotReady positions/offices |
| stg_model_predictions__llm_l2_br_match_20260126 | gemini outputs to string match L2 districts to BallotReady positions/offices |
| stg_model_predictions__manual_llm_election_results_20251208 | manually orchestrated LLM election decision results for candidacies sourced from m_general__candidacy |
| stg_model_predictions__turnout_projections_even_years_20250709 | Turnout projections data load from model predictions |
| stg_model_predictions__turnout_projections_model2odd | Turnout projections data load from model predictions |
| stg_model_predictions__viability_scores | Viability scores data load from model predictions |
| stg_nhgis__block_population | 2020 decennial census population per census block, unioned from the two research-staged NHGIS extracts. The file split is by row count, not state, so both tables are required for a complete national frame (Missouri spans both). Covers all states, DC, and Puerto Rico. The 2020 frame is static, so singular tests pin the exact national totals and Missouri's block count as union regressions. |
| stg_segment_storage_source__gp_api_account_password_reset_requested | (no description) |
| stg_segment_storage_source__gp_api_campaign_verify_token_status_update | (no description) |
| stg_segment_storage_source__gp_api_content_builder_generation_completed | (no description) |
| stg_segment_storage_source__gp_api_content_builder_generation_started | (no description) |
| stg_segment_storage_source__gp_api_identifies | Segment identify calls from GP API linking anonymous IDs to known users |
| stg_segment_storage_source__gp_api_onboarding_user_created | (no description) |
| stg_segment_storage_source__gp_api_peerly_identity_id_created | Segment event fired when a Peerly identity ID is created for 10DLC compliance |
| stg_segment_storage_source__gp_api_poll_results_synthesis_complete | (no description) |
| stg_segment_storage_source__gp_api_tracks | Segment track calls from GP API capturing user actions and events |
| stg_segment_storage_source__gp_api_users | Segment user profiles from GP API with traits and properties |
| stg_segment_storage_source__gp_api_voter_outreach_10dlc_compliance_completed | (no description) |
| stg_segment_storage_source__gp_api_voter_outreach_10dlc_compliance_pin_submitted | Segment event fired when a user submits their 10DLC compliance PIN |
| stg_segment_storage_source__web_app__10_dlc_compliance_pin_verification_completed | Segment web app event fired when 10DLC compliance PIN verification completes |
| stg_segment_storage_source__web_app__10_dlc_compliance_registration_submitted | (no description) |
| stg_segment_storage_source__web_app_account_password_reset_completed | (no description) |
| stg_segment_storage_source__web_app_ai_assistant_ask_a_question | (no description) |
| stg_segment_storage_source__web_app_ai_assistant_click_new_chat | (no description) |
| stg_segment_storage_source__web_app_ai_assistant_click_view_chat_history | (no description) |
| stg_segment_storage_source__web_app_ai_content_generation_start | (no description) |
| stg_segment_storage_source__web_app_campaign_assistant_chatbot_input | (no description) |
| stg_segment_storage_source__web_app_candidacy_did_you_win_modal_completed | (no description) |
| stg_segment_storage_source__web_app_candidacy_did_you_win_modal_viewed | (no description) |
| stg_segment_storage_source__web_app_candidate_website_continued | (no description) |
| stg_segment_storage_source__web_app_candidate_website_edited | (no description) |
| stg_segment_storage_source__web_app_candidate_website_published | (no description) |
| stg_segment_storage_source__web_app_candidate_website_purchased_domain | (no description) |
| stg_segment_storage_source__web_app_candidate_website_selected_domain | (no description) |
| stg_segment_storage_source__web_app_candidate_website_started | (no description) |
| stg_segment_storage_source__web_app_candidate_website_started_domain_selection | (no description) |
| stg_segment_storage_source__web_app_content_builder_click_content | (no description) |
| stg_segment_storage_source__web_app_content_builder_click_continue_questions | (no description) |
| stg_segment_storage_source__web_app_content_builder_click_generate | (no description) |
| stg_segment_storage_source__web_app_content_builder_close_additional_inputs | (no description) |
| stg_segment_storage_source__web_app_content_builder_editor_click_regenerate | (no description) |
| stg_segment_storage_source__web_app_content_builder_editor_open_version_picker | (no description) |
| stg_segment_storage_source__web_app_content_builder_editor_submit_regenerate | (no description) |
| stg_segment_storage_source__web_app_content_builder_select_template | (no description) |
| stg_segment_storage_source__web_app_content_builder_submit_additional_inputs | (no description) |
| stg_segment_storage_source__web_app_custom_voter_file_created | (no description) |
| stg_segment_storage_source__web_app_dashboard_candidate_dashboard_viewed | (no description) |
| stg_segment_storage_source__web_app_dashboard_path_to_victory_exit_understand_path_to_victory | (no description) |
| stg_segment_storage_source__web_app_download_voter_file_attempt | (no description) |
| stg_segment_storage_source__web_app_download_voter_file_failure | (no description) |
| stg_segment_storage_source__web_app_download_voter_file_success | (no description) |
| stg_segment_storage_source__web_app_exposure | (no description) |
| stg_segment_storage_source__web_app_identifies | Segment identify calls from web app linking anonymous IDs to known users |
| stg_segment_storage_source__web_app_invalid_party | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_ai_assistant | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_community | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_contacts | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_content_builder | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_dashboard | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_door_knocking | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_my_profile | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_polls | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_resources | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_voter_data | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_voter_outreach | (no description) |
| stg_segment_storage_source__web_app_navigation_dashboard_click_website | (no description) |
| stg_segment_storage_source__web_app_navigation_top_avatar_dropdown_click_logout | (no description) |
| stg_segment_storage_source__web_app_navigation_top_avatar_dropdown_click_settings | (no description) |
| stg_segment_storage_source__web_app_navigation_top_avatar_dropdown_close_dropdown | (no description) |
| stg_segment_storage_source__web_app_navigation_top_click_avatar_dropdown | (no description) |
| stg_segment_storage_source__web_app_navigation_top_click_logo | (no description) |
| stg_segment_storage_source__web_app_onboarding_candidate_affiliation_completed | (no description) |
| stg_segment_storage_source__web_app_onboarding_candidate_office_completed | (no description) |
| stg_segment_storage_source__web_app_onboarding_candidate_office_searched | (no description) |
| stg_segment_storage_source__web_app_onboarding_candidate_pledge_completed | (no description) |
| stg_segment_storage_source__web_app_onboarding_click_finish_later | (no description) |
| stg_segment_storage_source__web_app_onboarding_complete | (no description) |
| stg_segment_storage_source__web_app_onboarding_complete_step_click_go_to_dashboard | (no description) |
| stg_segment_storage_source__web_app_onboarding_office_step_click_can_t_see_office | (no description) |
| stg_segment_storage_source__web_app_onboarding_office_step_click_next | (no description) |
| stg_segment_storage_source__web_app_onboarding_office_step_office_selected | (no description) |
| stg_segment_storage_source__web_app_onboarding_party_step_click_submit | (no description) |
| stg_segment_storage_source__web_app_onboarding_pledge_step_click_submit | (no description) |
| stg_segment_storage_source__web_app_onboarding_registration_completed | (no description) |
| stg_segment_storage_source__web_app_outreach_action_clicked | (no description) |
| stg_segment_storage_source__web_app_outreach_click_create | (no description) |
| stg_segment_storage_source__web_app_outreach_door_knocking_complete | (no description) |
| stg_segment_storage_source__web_app_outreach_phone_banking_complete | (no description) |
| stg_segment_storage_source__web_app_outreach_view_accessed | (no description) |
| stg_segment_storage_source__web_app_p2p_upgrade_modal_click_button | (no description) |
| stg_segment_storage_source__web_app_p2p_upgrade_modal_exit | (no description) |
| stg_segment_storage_source__web_app_p2p_upgrade_modal_modal_shown | (no description) |
| stg_segment_storage_source__web_app_pages | Segment page view events from web app tracking user navigation |
| stg_segment_storage_source__web_app_polls_create_poll_clicked | (no description) |
| stg_segment_storage_source__web_app_polls_poll_question_viewed | (no description) |
| stg_segment_storage_source__web_app_polls_poll_results_issue_details_viewed | (no description) |
| stg_segment_storage_source__web_app_polls_poll_results_overview_viewed | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_click_exit_top_nav | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_click_go_to_stripe | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_committee_check_page_click_next | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_committee_check_page_click_upload | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_committee_check_page_hover_ein_number_help | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_committee_check_page_hover_name_of_campaign_committee_help | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_committee_check_page_hover_upload_help | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_committee_check_page_toggle_ein_requirement | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_complete | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_confirm_office | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_edit_office | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_modal_click_button | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_modal_exit | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_modal_modal_shown | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_service_agreement_page_click_finish | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_splash_page_click_upgrade | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_splash_page_exit | (no description) |
| stg_segment_storage_source__web_app_pro_upgrade_submit_edit_office | (no description) |
| stg_segment_storage_source__web_app_profile_campaign_details_click_save | (no description) |
| stg_segment_storage_source__web_app_profile_fun_fact_click_save | (no description) |
| stg_segment_storage_source__web_app_profile_office_details_click_edit | (no description) |
| stg_segment_storage_source__web_app_profile_running_against_cancel_add_new | (no description) |
| stg_segment_storage_source__web_app_profile_running_against_click_add_new | (no description) |
| stg_segment_storage_source__web_app_profile_running_against_click_edit | (no description) |
| stg_segment_storage_source__web_app_profile_running_against_click_save | (no description) |
| stg_segment_storage_source__web_app_profile_running_against_submit_add_new | (no description) |
| stg_segment_storage_source__web_app_profile_running_against_submit_edit | (no description) |
| stg_segment_storage_source__web_app_profile_top_issues_click_finish_entering_issues | (no description) |
| stg_segment_storage_source__web_app_profile_why_section_click_save | (no description) |
| stg_segment_storage_source__web_app_question_complete | (no description) |
| stg_segment_storage_source__web_app_resources_resource_clicked | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_audience_check_age | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_audience_check_audience | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_audience_check_gender | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_audience_check_political_party | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_audience_enter_audience_request | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_back | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_exit | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_next | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_script_click_add_your_own_script | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_script_click_generate_a_new_script | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_script_click_use_a_saved_script | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_script_select_saved_script | (no description) |
| stg_segment_storage_source__web_app_schedule_text_campaign_script_submit_added_script | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_add_image_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_constituency_profile_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_getting_started_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_meet_your_constituents_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_poll_preview_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_poll_strategy_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_poll_value_props_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_sms_poll_sent | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_success_page_viewed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_sworn_in_completed | (no description) |
| stg_segment_storage_source__web_app_serve_onboarding_sworn_in_viewed | (no description) |
| stg_segment_storage_source__web_app_set_password_click_set_password | (no description) |
| stg_segment_storage_source__web_app_settings_account_settings_click_upgrade | (no description) |
| stg_segment_storage_source__web_app_settings_delete_account_click_delete | (no description) |
| stg_segment_storage_source__web_app_settings_delete_account_submit_delete | (no description) |
| stg_segment_storage_source__web_app_settings_notifications_toggle_email | (no description) |
| stg_segment_storage_source__web_app_settings_personal_info_click_save | (no description) |
| stg_segment_storage_source__web_app_settings_personal_info_click_upload | (no description) |
| stg_segment_storage_source__web_app_sign_in_click_create_account | (no description) |
| stg_segment_storage_source__web_app_sign_in_click_forgot_password | (no description) |
| stg_segment_storage_source__web_app_sign_up_click_login | (no description) |
| stg_segment_storage_source__web_app_tracks | Segment track calls from web app capturing user actions and events |
| stg_segment_storage_source__web_app_users | Segment user profiles from web app with traits and properties |
| stg_segment_storage_source__web_app_usersnap_submission | (no description) |
| stg_segment_storage_source__web_app_voter_data_click_create_custom_voter_file | (no description) |
| stg_segment_storage_source__web_app_voter_data_click_detail_view | (no description) |
| stg_segment_storage_source__web_app_voter_data_custom_voter_file_click_create | (no description) |
| stg_segment_storage_source__web_app_voter_data_custom_voter_file_click_next | (no description) |
| stg_segment_storage_source__web_app_voter_data_custom_voter_file_exit_modal | (no description) |
| stg_segment_storage_source__web_app_voter_data_custom_voter_file_select_channel | (no description) |
| stg_segment_storage_source__web_app_voter_data_custom_voter_file_select_purpose | (no description) |
| stg_segment_storage_source__web_app_voter_data_file_detail_click_back | (no description) |
| stg_segment_storage_source__web_app_voter_data_file_detail_click_custom_file_info_icon | (no description) |
| stg_segment_storage_source__web_app_voter_data_file_detail_click_download_csv | (no description) |
| stg_segment_storage_source__web_app_voter_data_file_detail_click_view_audience_filters | (no description) |
| stg_segment_storage_source__web_app_voter_outreach_10dlc_compliance_form_submitted | (no description) |
| stg_segment_storage_source__web_app_voter_outreach_10dlc_compliance_started | (no description) |
| stg_segment_storage_source__web_app_voter_outreach_campaign_completed | (no description) |
| us_states | (no description) |
| users | Goodparty.org application users, with aggregate campaign counts appended. Grain: One row per user. Limitations: Win/loss outcome data requires future work and is not included in this MVP. |
| write__election_api_db | This model writes data to the election api database. |
| write__l2_databricks_to_gp_api | This model writes data from Databricks to the voter db in gp-api. |
| write__l2_databricks_to_people_api | This model writes data from Databricks to the people api database. |

## Output contract (required)

Answer inline in your final message. End the final message with:

1. An assumptions ledger: every scoping decision you made, one per line, each
   marked verified (you checked it against data/docs) or assumed.
2. A fenced yaml block, exactly this shape (your numbers/forks):

    ```yaml
    results:
      numbers:
        <metric_name>: <value>
      assumptions:
        - fork: <scoping_fork_name>
          resolution: <what you chose>
          verified: true|false
          source: <table/doc/query that justified it>
    ```

Number names: use the exact metric names the question asks for, snake_cased.
YAML values that contain a colon followed by a space (e.g. a parenthetical
like "breakdown: 149 x, 726 y") MUST be double-quoted, or the block fails to
parse as YAML and the whole answer is graded as missing. When in doubt, quote
the value.
