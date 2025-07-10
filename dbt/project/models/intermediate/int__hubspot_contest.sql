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
    properties_open_seat as seat_name,

-- to add
-- number_opponents,
-- seats_available,
-- uncontested,
-- open_seat,
-- filing_deadline,
-- primary_election_date,
-- general_election_date,
-- runoff_election_date,
-- primary_results,
-- general_election_results,
-- runoff_election_results,
-- contest_results,
-- term_start_date,
-- term_length_years,
-- contest_status
from {{ ref("stg_airbyte_source__hubspot_api_companies") }}
