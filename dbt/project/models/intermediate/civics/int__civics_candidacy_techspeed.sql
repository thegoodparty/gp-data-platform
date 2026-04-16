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

            primary_election_date_parsed as primary_election_date,
            general_election_date_parsed as general_election_date,
            cast(null as date) as primary_runoff_election_date,
            cast(null as date) as general_runoff_election_date,

            cast(null as float) as viability_score,
            cast(null as int) as win_number,
            cast(null as string) as win_number_model,

            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at

        from source
        left join
            canonical_candidacy as xw
            on source.techspeed_candidate_code = xw.ts_source_candidate_id
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
