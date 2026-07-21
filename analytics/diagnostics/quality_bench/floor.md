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
| campaigns | Goodparty.org application campaigns… |
| clean_states | (no description) |
| deid_voters | De-identified nationwide voter data for MBAN Analytics program (2026)… |
| election_api_race_filing_address_overrides | (no description) |
| fips_codes | (no description) |
| haystaq_issue_tags | (no description) |
| hubspot_call_dispositions | (no description) |
| hubspot_contact_property_columns | (no description) |
| icp_normalized_position_names | (no description) |
| int__amplitude_event_catalog | Read-only catalog of Amplitude events: macro-derived family classification plus volume/recency from the event stream, joined to Amplitude Govern metad… |
| int__amplitude_event_taxonomy | Single source of truth for Amplitude event-family classification… |
| int__amplitude_serve_activity | User x month intermediate aggregating Serve MAU activity from Amplitude… |
| int__amplitude_user_milestones | User-grain intermediate model that aggregates milestone events from stg_airbyte_source__amplitude_api_events into one row per user_id for analytics.us… |
| int__amplitude_win_activity | User x month intermediate aggregating Win product engagement from Amplitude… |
| int__amplitude_win_activity_weekly | User x week intermediate aggregating Win product engagement from Amplitude… |
| int__ballotready_candidacy | This model retrieves and processes candidate data from the CivicEngine API since the source candidacies_v3 csv file does not contain all the fields ne… |
| int__ballotready_candidate_identity | Canonical BallotReady candidate identity inputs, one deterministic row per br_candidate_id… |
| int__ballotready_endorsement | This model retrieves and processes candidate endorsement data from the CivicEngine API… |
| int__ballotready_filing_period | This model retrieves and processes filing period data from the CivicEngine API… |
| int__ballotready_geofence | This model retrieves and processes geofence data from the CivicEngine API. |
| int__ballotready_issue | This model retrieves and processes issue data from the CivicEngine API: https://developers.civicengine.com/docs/api/graphql/reference/objects/issue It… |
| int__ballotready_normalized_position | This model retrieves normalized position data from the CivicEngine API… |
| int__ballotready_party | This model retrieves and processes candidate endorsement data from the CivicEngine API… |
| int__ballotready_person | This model retrieves and processes person data from the CivicEngine API. |
| int__ballotready_position_election_frequency | This model retrieves and processes position election frequency data from the CivicEngine API… |
| int__ballotready_position_to_place | Intermediate model that maps positions to places from the BallotReady API |
| int__ballotready_stance | This model retrieves and processes candidate stance data from the CivicEngine API… |
| int__civics_candidacy_2025 | Historical archive of candidacies from the general mart with elections on or before 2025-12-31… |
| int__civics_candidacy_ballotready | BallotReady candidacies transformed into civics mart candidacy schema… |
| int__civics_candidacy_ddhq | DDHQ candidacies transformed into civics mart candidacy schema… |
| int__civics_candidacy_gp_api | Product Database campaigns transformed into the civics mart candidacy schema… |
| int__civics_candidacy_stage_2025 | Historical archive of candidacy stages (primary, general, general runoff) from elections on or before 2025-12-31… |
| int__civics_candidacy_stage_ballotready | BallotReady candidacy stages transformed into civics mart candidacy_stage schema… |
| int__civics_candidacy_stage_ddhq | DDHQ candidacy stages — the foundational DDHQ intermediate model… |
| int__civics_candidacy_stage_gp_api | Product Database campaigns × BR election_stage rows for same position+year… |
| int__civics_candidacy_stage_techspeed | TechSpeed candidacy stages transformed into civics mart candidacy_stage schema… |
| int__civics_candidacy_techspeed | TechSpeed candidates transformed into civics mart candidacy schema… |
| int__civics_candidate_2025 | Historical archive of candidates from the general mart with elections on or before 2025-12-31… |
| int__civics_candidate_ballotready | BallotReady candidates transformed into civics mart candidate schema… |
| int__civics_candidate_ddhq | DDHQ candidates transformed into civics mart candidate schema… |
| int__civics_candidate_gp_api | Product Database (gp_api_db) users transformed into the civics mart candidate schema… |
| int__civics_candidate_techspeed | TechSpeed candidates transformed into civics mart candidate schema… |
| int__civics_elected_official_ballotready | BallotReady office holders, term-grain… |
| int__civics_elected_official_ballotready_person | BR-only person-grain rollup of int__civics_elected_official_ballotready… |
| int__civics_elected_official_canonical_ids | Deterministic crosswalk: TechSpeed ts_officeholder_id ↔ BallotReady canonical IDs (term-grain UUID, person-grain UUID, raw BR keys)… |
| int__civics_elected_official_ddhq | (no description) |
| int__civics_elected_official_ddhq_matched_votes | DDHQ general-winner votes matched to each elected official via the matcha elected_official entity resolution: a DDHQ winner that shares a Splink clust… |
| int__civics_elected_official_gp_api | Product Database elected offices transformed into the civics mart elected_officials schema… |
| int__civics_elected_official_gp_api_bridge | Term-grain bridge from gp-api elected_office records to BR+TS terms, derived from the EO Splink cluster output… |
| int__civics_elected_official_gp_api_person | Person-grain rollup of gp-api elected officials… |
| int__civics_elected_official_techspeed | TechSpeed office holders transformed into the civics mart elected_officials schema… |
| int__civics_elected_official_techspeed_person | TS-side person-grain rollup… |
| int__civics_election_2025 | Historical archive of elections from the general mart with election dates on or before 2025-12-31… |
| int__civics_election_ballotready | BallotReady elections transformed into civics mart election schema… |
| int__civics_election_ddhq | DDHQ elections transformed into civics mart election schema… |
| int__civics_election_stage_2025 | Historical archive of election stages (primary, general, general runoff) from elections on or before 2025-12-31… |
| int__civics_election_stage_ballotready | BallotReady election stages transformed into civics mart election_stage schema… |
| int__civics_election_stage_ddhq | DDHQ election results transformed into civics mart election_stage schema… |
| int__civics_election_stage_techspeed | TechSpeed election stages transformed into civics mart election_stage schema… |
| int__civics_election_techspeed | TechSpeed elections transformed into civics mart election schema… |
| int__civics_er_canonical_election_stages | Direct election-stage ER crosswalk: maps DDHQ / TechSpeed election stages that clustered with a BallotReady race (gp-data-matcha election_stage entity… |
| int__civics_er_canonical_ids | Entity resolution crosswalk: provider raw keys → canonical gp_* IDs (BR's cluster-derived ids on BR-anchored clusters, the earliest-member mint on non… |
| int__civics_minted_candidacy_ids | Minted gp_candidacy_id per clustered candidacy-stage record… |
| int__civics_minted_election_stage_ids | Minted gp_election_stage_id per clustered election-stage record… |
| int__civics_person_canonical_ids | Canonical gp_person_id per record… |
| int__civics_person_edges | Deterministic person edges… |
| int__civics_person_groups | Person groups… |
| int__civics_person_nodes | Person record universe… |
| int__civics_position_office_type | One row per BallotReady position with the canonical office_type, derived from the normalized position name (DATA-1972)… |
| int__civics_viability_scoring | Viability scores for all candidacies in the civics mart, produced by an MLflow model waterfall (best-available model wins via COALESCE)… |
| int__ddhq_election_results_clean | Intermediate model that pulls selected properties from DDHQ election results |
| int__ddhq_election_results_embeddings | Intermediate model that creates embeddings for the election results |
| int__ddhq_races | Race-level union of DDHQ reported and upcoming races from the DDHQ Elections gsheet… |
| int__district_census_allocation | THE SUBSTRATE (DATA-1992, epic DATA-1359)… |
| int__enhanced_place | Intermediate model that enhances the place data from the BallotReady API |
| int__enhanced_place_w_parent | Intermediate model that enhances the place data from the BallotReady API with its parent place |
| int__enhanced_position | Intermediate model that enhances the position data from the BallotReady API |
| int__enhanced_position_w_parent | Intermediate model that enhances the position data from the BallotReady API with its parent position |
| int__enhanced_race | Intermediate model that enhances the race data from the BallotReady API |
| int__er_election_stage_candidacy_clusters | (no description) |
| int__er_prematch_candidacy_stages | Entity resolution prematch table: BallotReady x TechSpeed x DDHQ x GP API candidacy-stages unioned into a standardized schema for Splink probabilistic… |
| int__er_prematch_elected_officials | Entity resolution prematch table at term grain… |
| int__er_prematch_election_stages | Entity resolution prematch input for the matcha election_stage entity type… |
| int__general_candidacy | This model creates the candidacies object in the mart layer using the hubspot data |
| int__general_candidacy_clean_for_ddhq | This model creates the candidacies object in preparation of the mart layer using the candidacy data… |
| int__general_candidacy_embeddings_for_ddhq | Intermediate model that creates embeddings for the general candidacy data |
| int__general_states_zip_code_range | "Intermediate model that pulls selected properties from HubSpot contacts… |
| int__geo_id_attributes | Intermediate model that maps geo_id to its components and parent geo_id |
| int__gp_ai_candidacies | Intermediate model that creates the candidacies object in the intermediate layer using the HubSpot data… |
| int__gp_ai_election_match | DEPRECATED (DATA-1834): Frozen snapshot of DDHQ-HubSpot AI election match results from Dec 2, 2025… |
| int__gp_ai_start_election_match | Intermediate model that starts the election match process for GP AI |
| int__hs_companies_recent | (no description) |
| int__hs_companies_with_contacts | (no description) |
| int__hubspot_calls | HubSpot engagement calls, deduped to one row per call_id and enriched with the disposition label and outcome_family classification from the hubspot_ca… |
| int__hubspot_candidate_codes | "Intermediate model that generates unique candidate codes by concatenating and cleaning first name, last name, state, and office type from HubSpot con… |
| int__hubspot_companies_archive_2025 | Archived HubSpot companies prioritizing 2025 election dates… |
| int__hubspot_companies_w_contacts_2025 | Archived HubSpot companies joined with contacts from 2026-01-22 snapshot… |
| int__hubspot_contact_calls | One row per HubSpot contact_id with at least one logged call… |
| int__hubspot_contacts | "Intermediate model that pulls selected properties from HubSpot contacts… |
| int__hubspot_contacts_2025 | Archived HubSpot contacts from 2026-01-22 snapshot… |
| int__hubspot_contacts_archive_2025 | Archived HubSpot contacts prioritizing 2025 election dates… |
| int__hubspot_contacts_w_companies | "Intermediate model that pulls selected properties from HubSpot contacts… |
| int__hubspot_contest | "Intermediate model that pulls selected properties from HubSpot contacts… |
| int__hubspot_contest_2025 | (no description) |
| int__hubspot_prospect_contacts | One row per HubSpot contact, with lifecycle/stage/activation/pledge fields and resolved position-based ICP from int__hubspot_contacts… |
| int__icp_offices | ICP offices with voter counts per district (state, district type, and district name) |
| int__l2_block_district_map | One row per (census block, state, district_type, normalized district_name) with the L2 voter count in that intersection (voters_in_block_district) and… |
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
| int__serve_active_user | The single source of truth for the "active serve user" behavioral definition (epic DATA-1359), one row per user… |
| int__serve_block_coverage | The count-once engine (DATA-1993, epic DATA-1359)… |
| int__serve_district_resolution | Resolves each serve org to its L2 district: the serve-cohort entry point onto the District/Census substrate (a downstream consumer; election-api quara… |
| int__voter_turnout_lgbm_inference | Nationwide district-level voter-turnout projections from the LightGBM models (DATA-2015)… |
| int__zip_code_to_br_office | Zip code to BR office mapping |
| int__zip_code_to_l2_district | Zip code to L2 district mapping… |
| l2_br_match_overrides | (no description) |
| l2_column_classification | (no description) |
| load__l2_haystaq_s3_to_databricks | This model loads Haystaq issue model data from S3 to Databricks. |
| load__l2_haystaq_sftp_to_s3 | This model loads Haystaq issue model data from the L2 SFTP server to S3. |
| load__l2_s3_to_databricks | This model loads data from S3 to Databricks. |
| load__l2_sftp_to_s3 | This model loads data from the L2 SFTP server to S3. |
| m_election_api__candidacy | Mart model that serves the Election API |
| m_election_api__district | Mart model that serves districts to the Election API, part of projected turnout |
| m_election_api__district_top_issues | District-level Haystaq issue scores per L2 district, covering every L2 district with an `is_matched = true` row in the LLM L2-to-BallotReady district … |
| m_election_api__elected_official_support | Support metrics for the product's elected offices, one row per gp-api elected_office instance (elected_office_id, tied to a user/official)… |
| m_election_api__issue | Mart model that serves the Election API |
| m_election_api__place | Mart model that serves the Election API |
| m_election_api__position | Mart model that serves the Election API |
| m_election_api__projected_turnout | Turnout projections data load from model predictions |
| m_election_api__race | Mart model that serves the Election API |
| m_election_api__stance | Mart model that serves the Election API |
| m_election_api__zip_to_position | Maps zip codes to future election positions for the OfficePicker product… |
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
| m_people_api__districtstats | District statistics aggregating voter demographic data per district… |
| m_people_api__districtvoter | This model creates the district voter table in the mart layer using the voter table. |
| m_people_api__voter | This model creates the voter table in the mart layer using the hubspot data |
| metricflow_time_spine | Daily time spine for the dbt semantic layer (MetricFlow)… |
| nicknames | (no description) |
| serve_agent_voters_columns | (no description) |
| snapshot__hubspot_api_companies | (no description) |
| snapshot__hubspot_api_contacts | (no description) |
| snapshot__hubspot_api_deals | (no description) |
| snapshot__int__civics_person_canonical_ids | (no description) |
| snapshot__int__l2_nationwide_uniform | (no description) |
| states_zip_code_range | (no description) |
| stg_airbyte_internal__raw_gp_api_db_campaign | Parsed and deduplicated campaign data from Airbyte's insert-only raw stream… |
| stg_airbyte_source__amplitude_api_active_users | Staging model for Amplitude Active Users Counts stream - tracks daily active user counts with incremental sync |
| stg_airbyte_source__amplitude_api_annotations | Staging model for Amplitude Annotations stream - contains chart annotations and notes |
| stg_airbyte_source__amplitude_api_average_session_length | Staging model for Amplitude Average Session Length stream - tracks daily average session duration with incremental sync |
| stg_airbyte_source__amplitude_api_cohorts | Staging model for Amplitude Cohorts stream - contains user cohort definitions and metadata |
| stg_airbyte_source__amplitude_api_events | Staging model for Amplitude Events stream - contains individual user events with incremental sync |
| stg_airbyte_source__amplitude_api_events_list | Staging model for Amplitude Events List stream - contains metadata about tracked events |
| stg_airbyte_source__amplitude_taxonomy_event_type | Staging model for the Amplitude Govern taxonomy stream - one row per event_type with human-curated metadata… |
| stg_airbyte_source__ballotready_api_election | Election data scan from [`elections` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/elections) |
| stg_airbyte_source__ballotready_api_issue | Issue data scan from [`issues` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/issues) |
| stg_airbyte_source__ballotready_api_mtfcc | MTFCC data scan from [`mtfcc` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/mtfcc)… |
| stg_airbyte_source__ballotready_api_place | Place data scan from [`places` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/places) |
| stg_airbyte_source__ballotready_api_position | Position data scan from [`positions` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/positions) |
| stg_airbyte_source__ballotready_api_position_to_place | Position data scan from [`positions` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/positions) |
| stg_airbyte_source__ballotready_api_race | Race data scan from [`races` query](https://developers.civicengine.com/docs/api/graphql/reference/queries/races) |
| stg_airbyte_source__ballotready_s3_candidacies_v3 | Candidacies data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_office_holders_v3 | Office Holders data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_recruitment_v1 | Recruitment data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_uscities_v1_77 | US Cities data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ballotready_s3_uscounties_v1_73 | US Counties data load from ballotready csvs in s3 bucket |
| stg_airbyte_source__ddhq_elections_gsheet_reported_races | Race-level rows from the DDHQ Elections gsheet for races whose results have been reported… |
| stg_airbyte_source__ddhq_elections_gsheet_upcoming_races | Race-level rows from the DDHQ Elections gsheet for races whose results have not yet been reported… |
| stg_airbyte_source__ddhq_gdrive_election_results | Election results data load from ddhq google drive |
| stg_airbyte_source__ddhq_gdrive_election_results_invalid | DDHQ election results rows that fail data quality checks… |
| stg_airbyte_source__gp_api_db_annotation | Annotation records (notes, chats, bug reports) attached to EO Meeting Briefing artifacts… |
| stg_airbyte_source__gp_api_db_annotation_bug_report | raw data load from gp_api_db postgres db from table `annotation_bug_report` |
| stg_airbyte_source__gp_api_db_annotation_note | raw data load from gp_api_db postgres db from table `annotation_note` |
| stg_airbyte_source__gp_api_db_annotation_note_attachment | raw data load from gp_api_db postgres db from table `annotation_note_attachment` |
| stg_airbyte_source__gp_api_db_annotation_review | raw data load from gp_api_db postgres db from table `annotation_review` |
| stg_airbyte_source__gp_api_db_artifact_feedback | Positive/negative feedback submitted on EO Meeting Briefing artifacts… |
| stg_airbyte_source__gp_api_db_artifact_review | Automated/human review verdicts on EO artifacts (currently briefings)… |
| stg_airbyte_source__gp_api_db_campaign | raw data load from gp_api_db postgres db from table `campaign` |
| stg_airbyte_source__gp_api_db_campaign_position | raw data load from gp_api_db postgres db from table `campaign_position` |
| stg_airbyte_source__gp_api_db_chat_conversation | Chat conversations collected from EO Meeting Briefings… |
| stg_airbyte_source__gp_api_db_chat_message | raw data load from gp_api_db postgres db from table `chat_message` |
| stg_airbyte_source__gp_api_db_community_issue | Community issues surfaced for an organization, ranked and categorized… |
| stg_airbyte_source__gp_api_db_domain | raw data load from gp_api_db postgres db from table `domain` |
| stg_airbyte_source__gp_api_db_ecanvasser | Staging model for eCanvasser stream - contains eCanvasser configuration and sync information |
| stg_airbyte_source__gp_api_db_ecanvasser_contact | Staging model for eCanvasser Contact stream - contains voter contact information from eCanvasser |
| stg_airbyte_source__gp_api_db_ecanvasser_house | Staging model for eCanvasser House stream - contains household information from eCanvasser |
| stg_airbyte_source__gp_api_db_ecanvasser_interaction | Staging model for eCanvasser Interaction stream - contains interaction records between contacts and canvassers |
| stg_airbyte_source__gp_api_db_elected_office | raw data load from gp_api_db postgres db from table `elected_office` |
| stg_airbyte_source__gp_api_db_election_type | raw data load from gp_api_db postgres db from table `election_type` |
| stg_airbyte_source__gp_api_db_experiment_run | Experiment runs from the gp-api experiment pipeline… |
| stg_airbyte_source__gp_api_db_meeting_briefing | EO Meeting Briefing artifacts generated for an elected office and meeting date… |
| stg_airbyte_source__gp_api_db_organization | raw data load from gp_api_db postgres db from table `organization` |
| stg_airbyte_source__gp_api_db_outreach | Staging model for Outreach stream - contains outreach campaign information and configuration |
| stg_airbyte_source__gp_api_db_path_to_victory | raw data load from gp_api_db postgres db from table `path_to_victory` |
| stg_airbyte_source__gp_api_db_poll | raw data load from gp_api_db postgres db from table `poll` |
| stg_airbyte_source__gp_api_db_poll_individual_message | raw data load from gp_api_db postgres db from table `poll_individual_message` |
| stg_airbyte_source__gp_api_db_poll_issues | raw data load from gp_api_db postgres db from table `poll_issues` |
| stg_airbyte_source__gp_api_db_position | raw data load from gp_api_db postgres db from table `position` |
| stg_airbyte_source__gp_api_db_race_opponent_contrast | AI-generated candidate/opponent contrast statements for a race… |
| stg_airbyte_source__gp_api_db_race_opponent_research | Opponent research runs for a race… |
| stg_airbyte_source__gp_api_db_race_opponent_standout_action | AI-generated standout actions attributed to an opponent… |
| stg_airbyte_source__gp_api_db_race_opponent_summary | AI-generated opponent research summaries for a race… |
| stg_airbyte_source__gp_api_db_tcr_compliance | raw data load from gp_api_db postgres db from table `tcr_compliance` |
| stg_airbyte_source__gp_api_db_top_issue | raw data load from gp_api_db postgres db from table `top_issue` |
| stg_airbyte_source__gp_api_db_user | raw data load from gp_api_db postgres db from table `user` |
| stg_airbyte_source__gp_api_db_website | raw data load from gp_api_db postgres db from table `website` |
| stg_airbyte_source__gp_api_db_website_contact | raw data load from gp_api_db postgres db from table `website_contact` |
| stg_airbyte_source__gp_api_db_website_view | raw data load from gp_api_db postgres db from table `website_view` |
| stg_airbyte_source__hubspot_api_companies | Companies data load from HubSpot API |
| stg_airbyte_source__hubspot_api_contacts | Contacts data loaded from the HubSpot API… |
| stg_airbyte_source__hubspot_api_deals | Deals data load from HubSpot API |
| stg_airbyte_source__hubspot_api_engagements | Engagements data load from HubSpot API |
| stg_airbyte_source__hubspot_api_engagements_calls | Staged HubSpot Call engagements… |
| stg_airbyte_source__hubspot_api_feedback_submissions | Staged HubSpot Feedback Survey submissions… |
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
| stg_airbyte_source__stripe_api_events | Staging model for Stripe Events stream - contains webhook events and API activity logs with Incremental sync… |
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
| stg_airbyte_source__techspeed_gdrive_candidates | Candidates data from TechSpeed Google Drive… |
| stg_airbyte_source__techspeed_gdrive_candidates_invalid | TechSpeed candidate rows that fail data quality checks… |
| stg_airbyte_source__techspeed_gdrive_marketing_data_enrichment | Marketing data enrichment load from TechSpeed Google Drive |
| stg_airbyte_source__techspeed_gdrive_officeholders | Officeholders data load from TechSpeed Google Drive |
| stg_airflow_source__l2_expired_voters | Staged view of expired L2 voter IDs ingested by the Airflow l2_expired_voters DAG… |
| stg_airflow_source__l2_expired_voters_loads | Load metadata for the l2_expired_voters DAG… |
| stg_census__block_population | 2020 decennial census population per census block, unioned from the two research-staged NHGIS extracts… |
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
| stg_er_source__clustered_candidacy_stages | Staged cluster assignments from the Splink entity resolution pipeline… |
| stg_er_source__clustered_elected_officials | Match groups bridging gp-api elected-office records to BR+TS terms… |
| stg_er_source__clustered_election_stages | Staged cluster assignments from the election-stage Splink entity resolution pipeline… |
| stg_er_source__pairwise_candidacy_stages | Staged pairwise predictions from the Splink entity resolution pipeline… |
| stg_er_source__pairwise_elected_officials | Staged pairwise predictions from the elected officials entity resolution pipeline… |
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
| stg_nhgis__block_population | 2020 decennial census population per census block, unioned from the two research-staged NHGIS extracts… |
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
| users | Goodparty.org application users, with aggregate campaign counts appended… |
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
