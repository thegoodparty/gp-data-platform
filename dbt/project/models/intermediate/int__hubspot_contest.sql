{{
    config(
        materialized="incremental",
        unique_key="contact_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["intermediate", "candidacy", "contest", "hubspot"],
    )
}}

select
    id as contact_id,

    -- office information
    properties_official_office_name as official_office_name,
    properties_candidate_office as candidate_office,
    properties_office_type as office_type,
    properties_office_level as office_level,
    properties_partisan_type as partisan_type,

    -- geographic information
    properties_state as state,
    properties_city as city,
    properties_candidate_district as district,
    coalesce(
        case
            when properties_official_office_name like '% - Seat %'
            then regexp_extract(properties_official_office_name, ' - Seat ([^,]+)')
            when properties_official_office_name like '% - Group %'
            then regexp_extract(properties_official_office_name, ' - Group ([^,]+)')
            when properties_official_office_name like '%, Seat %'
            then regexp_extract(properties_official_office_name, ', Seat ([^,]+)')
            when properties_official_office_name like '%: % - Seat %'
            then regexp_extract(properties_official_office_name, ' - Seat ([^,]+)')
            when properties_official_office_name like '% - Position %'
            then
                regexp_extract(
                    properties_official_office_name, ' - Position ([^\\s(]+)'
                )
            else null
        end,
        -- properties_candidate_seat,  # add in once updated in hubspot
        ''
    ) as seat_name,

    -- election context
    properties_number_opponents as number_of_opponents,
    properties_number_of_seats_available as seats_available,
    properties_uncontested as uncontested,
    properties_open_seat as open_seat,
    properties_start_date as term_start_date,

    -- election dates
    properties_filing_deadline as filing_deadline,
    properties_primary_election_date as primary_election_date,
    properties_general_election_date as general_election_date,
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
    `updatedAt` as updated_at,
    `createdAt` as created_at

-- TODO: add term_length_years when available
-- TODO: add tests
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
where
    1 = 1
    and properties_official_office_name is not null
    and properties_office_type is not null
    and properties_candidate_office is not null
    {% if is_incremental() %}
        and `updatedAt` >= (select max(updated_at) from {{ this }})
    {% endif %}
