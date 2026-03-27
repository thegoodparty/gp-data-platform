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
    clean_states as (select * from {{ ref("clean_states") }}),

    source as (
        select
            ts.* except (state),
            coalesce(cs.state_cleaned_postal_code, ts.state) as state,
            cast(null as string) as seat_name,
            try_cast(number_of_seats_available as int) as seats_available,
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM/dd/yy'),
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM/dd/yy')
            ) as election_date
        from {{ ref("stg_airbyte_source__techspeed_gdrive_candidates") }} as ts
        left join
            clean_states as cs on upper(trim(ts.state)) = upper(trim(cs.state_raw))
    ),

    with_election_id as (
        select {{ generate_gp_election_id() }} as gp_election_id, source.*
        from source
        where election_date is not null and year(election_date) between 1900 and 2030
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
                    case when general_election_date is not null then 0 else 1 end,
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
            case
                when
                    year(
                        coalesce(
                            try_cast(filing_deadline as date),
                            try_to_date(filing_deadline, 'MM-dd-yyyy')
                        )
                    )
                    between 1900 and 2030
                then
                    coalesce(
                        try_cast(filing_deadline as date),
                        try_to_date(filing_deadline, 'MM-dd-yyyy')
                    )
                else null
            end as filing_deadline,
            try_cast(
                trim(regexp_replace(cast(population as string), '[^0-9]', '')) as int
            ) as population,
            seats_available,
            cast(null as date) as term_start_date,
            -- String-typed to match existing mart contract
            case
                when
                    upper(trim(cast(is_uncontested as string)))
                    in ('UNCONTESTED', 'YES', 'TRUE')
                then 'true'
                when
                    upper(trim(cast(is_uncontested as string)))
                    in ('CONTESTED', 'NO', 'FALSE')
                then 'false'
                else null
            end as is_uncontested,
            cast(null as string) as number_of_opponents,
            case
                when upper(trim(cast(open_seat as string))) in ('YES', 'TRUE')
                then 'true'
                when upper(trim(cast(open_seat as string))) in ('NO', 'FALSE')
                then 'false'
                else null
            end as open_seat,
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
