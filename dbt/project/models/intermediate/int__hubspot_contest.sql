{{
    config(
        auto_liquid_cluster=true,
        tags=["intermediate", "candidacy", "contest", "hubspot"],
    )
}}

select
    id as contact_id,

    -- office information
    official_office_name,
    candidate_office,
    office_type,
    office_level,
    partisan_type,

    -- geographic information
    state,
    city,
    candidate_district as district,
    coalesce(
        case
            when official_office_name like '% - Seat %'
            then regexp_extract(official_office_name, ' - Seat ([^,]+)')
            when official_office_name like '% - Group %'
            then regexp_extract(official_office_name, ' - Group ([^,]+)')
            when official_office_name like '%, Seat %'
            then regexp_extract(official_office_name, ', Seat ([^,]+)')
            when official_office_name like '%: % - Seat %'
            then regexp_extract(official_office_name, ' - Seat ([^,]+)')
            when official_office_name like '% - Position %'
            then regexp_extract(official_office_name, ' - Position ([^\\s(]+)')
            else null
        end,
        -- properties_candidate_seat,  # add in once updated in hubspot
        ''
    ) as seat_name,
    population,

    -- election context
    number_opponents as number_of_opponents,
    number_of_seats_available as seats_available,
    uncontested,
    open_seat,
    start_date as term_start_date,
    election_date,
    cast(left(election_date, 4) as integer) as election_year,

    -- election dates
    filing_deadline,
    primary_election_date,
    general_election_date,
    cast(null as date) as runoff_election_date,  -- get from campaign

    -- election results
    case
        when current_date() < filing_deadline
        then 'not started'
        when
            current_date() >= filing_deadline
            and current_date() <= case
                when runoff_election_date is null
                then general_election_date
                when general_election_date is null
                then runoff_election_date
                else greatest(runoff_election_date, general_election_date)
            end
        then 'in progress'
        when
            current_date() > case
                when runoff_election_date is null
                then general_election_date
                when general_election_date is null
                then runoff_election_date
                else greatest(runoff_election_date, general_election_date)
            end
        then 'completed'
    end as contest_status,
    cast(null as string) as primary_results,
    cast(null as string) as general_election_results,
    cast(null as string) as runoff_election_results,
    cast(null as string) as contest_results,

    -- metadata
    updated_at,
    created_at

from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
where
    1 = 1
    and official_office_name is not null
    and office_type is not null
    and candidate_office is not null
