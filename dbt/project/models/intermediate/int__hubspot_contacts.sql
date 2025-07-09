{{
    config(
        materialized="incremental",
        unique_key="id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
        tags=["intermediate", "candidacy", "contacts", "hubspot"],
    )
}}

select
    id,

    -- Personal information
    properties_full_name as full_name,
    properties_firstname as first_name,
    properties_lastname as last_name,
    properties_birth_date as birth_date,
    properties_email as email,
    properties_email as email_contacts,
    properties_phone as phone_number,
    properties_website as website_url,
    properties_instagram_handle as instagram_handle,
    properties_linkedin_url as linkedin_url,
    properties_twitterhandle as twitter_handle,
    properties_facebook_url as facebook_url,
    properties_address as street_address,
    properties_candidate_id_source as candidate_id_source,
    properties_candidate_id_tier as candidate_id_tier,

    -- Office information
    properties_official_office_name as official_office_name,
    properties_candidate_office as candidate_office,
    properties_office_level as office_level,
    properties_office_type as office_type,
    properties_party_affiliation as party_affiliation,
    properties_partisan_type as is_partisan,

    -- Geographic information
    properties_state as state,
    properties_city as city,
    properties_candidate_district as district,
    properties_open_seat as seat,
    try_cast(properties_population as int) as population,

    -- Election dates
    properties_filing_deadline as filing_deadline,
    properties_primary_election_date as primary_election_date,
    coalesce(
        properties_election_date, properties_general_election_date
    ) as general_election_date,
    cast(null as date) as runoff_election_date,

    -- Election context
    properties_incumbent as is_incumbent,
    properties_uncontested as is_uncontested,
    properties_number_opponents as number_of_opponents,

    -- Metadata
    `createdAt` as created_at,
    `updatedAt` as updated_at
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
where
    1 = 1 and properties_firstname is not null and properties_lastname is not null
    {% if is_incremental() %}
        and `updatedAt` >= (select max(updated_at) from {{ this }})
    {% endif %}
