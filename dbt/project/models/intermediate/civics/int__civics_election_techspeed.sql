{{ config(materialized="table", tags=["civics", "techspeed"]) }}

-- TechSpeed candidates → Civics mart election schema
-- Source: int__techspeed_candidates_clean (candidate-level)
--
-- Grain: One row per election (position + election year)
--
-- Derived directly from source data (not from election_stage) because we need
-- source-level fields like population, filing_deadline, and is_uncontested that
-- aren't carried through the election_stage model.
with
    clean_states as (select * from {{ ref("clean_states") }}),

    source as (
        select
            ts.* except (election_date, state),
            coalesce(cs.state_cleaned_postal_code, ts.state) as state,
            cast(null as string) as seat_name,
            try_cast(number_of_seats_available as int) as seats_available,
            coalesce(
                try_cast(ts.general_election_date as date),
                try_to_date(ts.general_election_date, 'MM/dd/yyyy'),
                try_to_date(ts.general_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.general_election_date, 'MM/dd/yy'),
                try_cast(ts.primary_election_date as date),
                try_to_date(ts.primary_election_date, 'MM/dd/yyyy'),
                try_to_date(ts.primary_election_date, 'MM-dd-yyyy'),
                try_to_date(ts.primary_election_date, 'MM/dd/yy')
            ) as election_date
        from {{ ref("int__techspeed_candidates_clean") }} as ts
        left join
            clean_states as cs on upper(trim(ts.state)) = upper(trim(cs.state_raw))
        where
            ts.first_name is not null
            and ts.last_name is not null
            and ts.state is not null
            and ts.techspeed_candidate_code is not null
    ),

    elections as (
        select
            {{ generate_gp_election_id() }} as gp_election_id,
            any_value(official_office_name) as official_office_name,
            any_value(candidate_office) as candidate_office,
            any_value(office_level) as office_level,
            any_value(office_type) as office_type,
            any_value(state) as state,
            any_value(city) as city,
            any_value(district) as district,
            any_value(seat_name) as seat_name,
            any_value(election_date) as election_date,
            any_value(year(election_date)) as election_year,
            any_value(
                case
                    when
                        year(
                            coalesce(
                                try_cast(filing_deadline as date),
                                try_to_date(filing_deadline, 'MM/dd/yyyy'),
                                try_to_date(filing_deadline, 'MM-dd-yyyy')
                            )
                        )
                        between 1900 and 2030
                    then
                        coalesce(
                            try_cast(filing_deadline as date),
                            try_to_date(filing_deadline, 'MM/dd/yyyy'),
                            try_to_date(filing_deadline, 'MM-dd-yyyy')
                        )
                    else null
                end
            ) as filing_deadline,
            any_value(population) as population,
            any_value(seats_available) as seats_available,
            cast(null as date) as term_start_date,
            any_value(
                case
                    when
                        upper(trim(cast(uncontested as string)))
                        in ('UNCONTESTED', 'YES', 'TRUE')
                    then true
                    when
                        upper(trim(cast(uncontested as string)))
                        in ('CONTESTED', 'NO', 'FALSE')
                    then false
                    else null
                end
            ) as is_uncontested,
            cast(null as string) as number_of_opponents,
            any_value(
                case
                    when upper(trim(cast(open_seat as string))) in ('YES', 'TRUE')
                    then true
                    when upper(trim(cast(open_seat as string))) in ('NO', 'FALSE')
                    then false
                    else null
                end
            ) as open_seat,
            false as has_ddhq_match,
            cast(null as bigint) as br_position_database_id,
            cast(null as boolean) as is_judicial,
            cast(null as boolean) as is_appointed,
            cast(null as string) as br_normalized_position_type,
            min(_airbyte_extracted_at) as created_at,
            max(_airbyte_extracted_at) as updated_at
        from source
        where election_date is not null and year(election_date) between 1900 and 2030
        group by gp_election_id
    )

select *
from elections
