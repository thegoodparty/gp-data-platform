{{
    config(
        materialized="incremental",
        unique_key="contact_id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["intermediate", "candidacy", "contest", "hubspot"],
    )
}}

/* generate unique gp_contest_id_v1 from the following fields:
official_office_name
candidate_office
office_type
office_level
state
city
district
seat_name
*/
select
    id as contact_id,
    -- gp_contest_id_v1
    -- office information
    properties_official_office_name as official_office_name,
    properties_candidate_office as candidate_office,
    properties_office_type as office_type,
    properties_office_level as office_level,
    properties_partisanship_type as partisanship_type,

    -- geographic information
    properties_state as state,
    properties_city as city,
    properties_candidate_district as district,
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
        then regexp_extract(properties_official_office_name, ' - Position ([^\\s(]+)')
        else ''
    end as seat_name,

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
    properties_runoff_election_date as runoff_election_date,
    cast(null as date) as runoff_election_date,  -- get from campaign

    -- election results
    case
        when today() < filing_deadline
        then 'not started'
        when
            today() >= filing_deadline
            and today() <= max(general_election_date, runoff_election_date)
        then 'in progress'
        when today() > max(general_election_date, runoff_election_date)
        then 'completed'
    end as contest_status,
    null as primary_results,
    null as general_election_results,
    null as runoff_election_results,
    null as contest_results

-- to add
-- term_length_years,
-- TODO: add tests
from {{ ref("stg_airbyte_source__hubspot_api_companies") }}
where
    1 = 1 and properties_official_office_name is not null
    {% if is_incremental() %}
        and `updatedAt` >= (select max(updated_at) from {{ this }})
    {% endif %}
