{{
    config(
        materialized="table",
        tags=["intermediate", "civics", "contest", "hubspot", "archive"],
    )
}}

-- Archived HubSpot contest data from 2026-01-22 snapshot
-- This model uses the archived staging data to ensure historical consistency
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
    -- Extract seat/group/position identifier from official_office_name
    -- Examples: "City Council - Seat 3" → "3", "School Board, Seat A" → "A"
    -- Each pattern has different capture rules:
    -- - Seat/Group: capture until comma
    -- - Position: capture until whitespace or parenthesis
    coalesce(
        regexp_extract(properties_official_office_name, ' - (?:Seat|Group) ([^,]+)'),
        regexp_extract(properties_official_office_name, ', Seat ([^,]+)'),
        regexp_extract(properties_official_office_name, ' - Position ([^\\s(]+)'),
        ''
    ) as seat_name,
    cast(
        cast(nullif(trim(cast(properties_population as string)), '') as decimal) as int
    ) as population,

    -- election context
    properties_number_opponents as number_of_opponents,
    properties_number_of_seats_available as seats_available,
    properties_uncontested as uncontested,
    properties_open_seat as open_seat,
    properties_start_date as term_start_date,
    properties_election_date as election_date,
    cast(left(properties_election_date, 4) as integer) as election_year,

    -- election dates
    properties_filing_deadline as filing_deadline,
    properties_primary_election_date as primary_election_date,
    properties_general_election_date as general_election_date,
    cast(null as date) as runoff_election_date,

    -- metadata
    updated_at,
    created_at

from {{ ref("int__hubspot_contacts_archive_2025") }}
where
    1 = 1
    and properties_official_office_name is not null
    and properties_office_type is not null
    and properties_candidate_office is not null
