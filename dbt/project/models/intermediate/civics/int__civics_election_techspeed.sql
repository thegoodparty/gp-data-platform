{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart election schema
-- Source: stg_airbyte_source__techspeed_gdrive_candidates
--
-- Grain: One row per election (position + election year)
--
-- Uses a representative-row approach (not any_value aggregation) to ensure
-- deterministic output. Picks one candidate per gp_election_id, preferring
-- rows with a general election date and the latest extraction timestamp.
with
    source as (
        select
            ts.* except (state),
            -- Aliases for generate_gp_election_id macro compatibility
            state_postal_code as state,
            cast(null as string) as seat_name
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
    ),

    with_election_id as (
        select {{ generate_gp_election_id() }} as gp_election_id, source.*
        from source
        where election_date is not null and year(election_date) between 1900 and 2050
    ),

    -- Pick one representative row per election. Prefer rows with a general
    -- election date (more complete data), then break ties with latest extraction.
    representative_row as (
        select *
        from with_election_id
        qualify
            row_number() over (
                partition by gp_election_id
                order by
                    case
                        when general_election_date_parsed is not null then 0 else 1
                    end,
                    _airbyte_extracted_at desc,
                    first_name,
                    last_name
            )
            = 1
    ),

    elections as (
        select
            gp_election_id,
            official_office_name,
            candidate_office,
            office_level,
            office_type,
            state,
            city,
            district,
            seat_name,
            election_date,
            year(election_date) as election_year,
            filing_deadline_parsed as filing_deadline,
            population,
            seats_available,
            cast(null as date) as term_start_date,
            -- Keep boolean type to match BallotReady election intermediate
            is_uncontested,
            cast(null as string) as number_of_opponents,
            is_open_seat,
            false as has_ddhq_match,
            cast(null as bigint) as br_position_database_id,
            cast(null as boolean) as is_judicial,
            cast(null as boolean) as is_appointed,
            cast(null as string) as br_normalized_position_type,
            _airbyte_extracted_at as created_at,
            _airbyte_extracted_at as updated_at
        from representative_row
    )

select *
from elections
