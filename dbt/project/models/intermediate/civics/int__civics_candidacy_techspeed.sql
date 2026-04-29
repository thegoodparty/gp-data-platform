{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart candidacy schema
-- Source: stg_airbyte_source__techspeed_gdrive_candidates (preserves candidacy-stage
-- grain)
--
-- CRITICAL: UUID fields MUST match int__hubspot_companies_w_contacts_2025 pattern
-- to ensure same candidacy from different sources gets same gp_candidacy_id
with
    source as (
        select
            ts.* except (state),
            -- Alias for generate_gp_election_id macro compatibility
            state_postal_code as state,
            cast(null as string) as seat_name,
            -- Generate candidate code inline (was provided by _clean)
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
    ),

    -- ER crosswalk: for clustered TS candidacies, adopt BR's canonical gp_* IDs.
    -- Deduped to one row per source_candidate_id — all stages of a matched
    -- candidacy share the same BR candidacy/candidate/election IDs.
    canonical_candidacy as (
        select
            ts_source_candidate_id,
            canonical_gp_candidacy_id,
            canonical_gp_candidate_id,
            canonical_gp_election_id
        from {{ ref("int__civics_er_canonical_ids") }}
        qualify
            row_number() over (
                partition by ts_source_candidate_id order by canonical_gp_candidacy_id
            )
            = 1
    ),

    -- BR enrichment: when TS captures a br_race_id (100% populated in staging),
    -- look up the BR-side stage to recover br_position_database_id and the
    -- BR-side gp_election_id. This unlocks ICP join + sibling-stage date
    -- lookup for TS-only candidacies that lack a BR candidacy row.
    br_race_to_position as (
        select
            br_race_id,
            cast(br_position_id as bigint) as br_position_database_id,
            gp_election_id as br_gp_election_id
        from {{ ref("int__civics_election_stage_ballotready") }}
        where br_position_id is not null
        qualify
            row_number() over (partition by br_race_id order by election_date desc) = 1
    ),

    -- Pivot all stages of a BR-side election into per-stage_type dates so we
    -- can fill in missing TS form dates (TS captures only primary or general,
    -- never runoffs).
    br_election_dates as (
        select
            gp_election_id as br_gp_election_id,
            max(
                case when stage_type = 'primary' then election_date end
            ) as br_primary_election_date,
            max(
                case when stage_type = 'general' then election_date end
            ) as br_general_election_date,
            max(
                case when stage_type = 'primary runoff' then election_date end
            ) as br_primary_runoff_election_date,
            max(
                case when stage_type = 'general runoff' then election_date end
            ) as br_general_runoff_election_date
        from {{ ref("int__civics_election_stage_ballotready") }}
        group by gp_election_id
    ),

    candidacies as (
        -- gp_candidate_id and gp_election_id use WINDOW propagation so all raw
        -- rows of the same person/election adopt BR's canonical if ANY candidacy
        -- of that person/election was matched — matching the propagation used
        -- in int__civics_candidate_techspeed and int__civics_election_techspeed
        -- (required for FK relationships to hold).
        -- gp_candidacy_id is per-row (each candidacy matches its own xw entry).
        select
            coalesce(
                xw.canonical_gp_candidacy_id, {{ generate_ts_gp_candidacy_id() }}
            ) as gp_candidacy_id,

            cast(null as string) as br_candidacy_id,

            coalesce(
                max(xw.canonical_gp_candidate_id) over (
                    partition by {{ generate_ts_gp_candidate_id() }}
                ),
                {{ generate_ts_gp_candidate_id() }}
            ) as gp_candidate_id,

            coalesce(
                max(xw.canonical_gp_election_id) over (
                    partition by {{ generate_gp_election_id() }}
                ),
                {{ generate_gp_election_id() }}
            ) as gp_election_id,

            cast(null as string) as product_campaign_id,
            cast(null as string) as hubspot_contact_id,
            cast(null as string) as hubspot_company_ids,

            'techspeed' as candidate_id_source,
            techspeed_candidate_code as candidate_code,

            party as party_affiliation,
            is_incumbent,
            is_open_seat,
            candidate_office,
            official_office_name,
            office_level,

            false as is_pledged,
            true as is_verified,
            cast(null as string) as verification_status_reason,
            is_partisan,

            -- Election results are tracked at the candidacy_stage level, not here
            cast(null as string) as candidacy_result,

            coalesce(
                primary_election_date_parsed, bed.br_primary_election_date
            ) as primary_election_date,
            coalesce(
                general_election_date_parsed, bed.br_general_election_date
            ) as general_election_date,
            bed.br_primary_runoff_election_date as primary_runoff_election_date,
            bed.br_general_runoff_election_date as general_runoff_election_date,

            brp.br_position_database_id,

            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,

            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from source
        left join
            canonical_candidacy as xw
            on source.techspeed_candidate_code = xw.ts_source_candidate_id
        left join br_race_to_position as brp on source.br_race_id = brp.br_race_id
        left join
            br_election_dates as bed on brp.br_gp_election_id = bed.br_gp_election_id
        where
            techspeed_candidate_code is not null
            and coalesce(general_election_date_parsed, primary_election_date_parsed)
            is not null
            and first_name is not null
            and last_name is not null
            and state is not null
    ),

    deduplicated as (
        select *
        from candidacies
        qualify
            row_number() over (
                partition by gp_candidacy_id
                order by updated_at desc, candidate_code asc nulls last
            )
            = 1
    )

select *
from deduplicated
