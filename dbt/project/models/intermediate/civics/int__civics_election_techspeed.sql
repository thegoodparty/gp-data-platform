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
    -- ER crosswalk: for clustered TS candidacies, adopt BR's canonical election_id.
    -- Deduped per source_candidate_id; any match for this candidacy gives BR's id.
    canonical_election as (
        select distinct ts_source_candidate_id, canonical_gp_election_id
        from {{ ref("int__civics_er_canonical_ids") }}
    ),

    source as (
        select
            ts.* except (state),
            -- Aliases for generate_gp_election_id macro compatibility
            state_postal_code as state,
            cast(null as string) as seat_name,
            {{
                generate_candidate_code(
                    "ts.first_name",
                    "ts.last_name",
                    "ts.state_postal_code",
                    "ts.office_type",
                    "ts.city",
                )
            }} as techspeed_candidate_code
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
    ),

    with_election_id as (
        -- If ANY candidacy in an election was clustered to BR, all rows of that
        -- election adopt BR's canonical_gp_election_id (propagated via a window
        -- partitioned by the raw TS hash). Otherwise keep the TS hash.
        select
            coalesce(
                max(xw.canonical_gp_election_id) over (
                    partition by {{ generate_gp_election_id() }}
                ),
                {{ generate_gp_election_id() }}
            ) as gp_election_id,
            source.*
        from source
        left join
            canonical_election as xw
            on source.techspeed_candidate_code = xw.ts_source_candidate_id
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
