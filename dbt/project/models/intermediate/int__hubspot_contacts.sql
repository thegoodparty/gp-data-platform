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
    -- identifiers and relations
    tbl_hs_contacts.id,
    case
        when tbl_hs_contacts.companies is null
        then null
        when trim(tbl_hs_contacts.companies) = ''
        then null
        when trim(tbl_hs_contacts.companies) = '[]'
        then null
        else from_json(tbl_hs_contacts.companies, 'array<string>')
    end as companies,  -- type is array<string>

    -- Personal information
    tbl_hs_contacts.properties_full_name as full_name,
    tbl_hs_contacts.properties_firstname as first_name,
    tbl_hs_contacts.properties_lastname as last_name,
    tbl_hs_contacts.properties_birth_date as birth_date,
    tbl_hs_contacts.properties_email as email,
    tbl_hs_contacts.properties_email as email_contacts,
    tbl_hs_contacts.properties_phone as phone_number,
    tbl_hs_contacts.properties_website as website_url,
    tbl_hs_contacts.properties_instagram_handle as instagram_handle,
    tbl_hs_contacts.properties_linkedin_url as linkedin_url,
    tbl_hs_contacts.properties_twitterhandle as twitter_handle,
    tbl_hs_contacts.properties_facebook_url as facebook_url,
    tbl_hs_contacts.properties_address as street_address,
    tbl_hs_contacts.properties_candidate_id_source as candidate_id_source,
    tbl_hs_contacts.properties_candidate_id_tier as candidate_id_tier,

    -- Office information
    tbl_hs_contacts.properties_official_office_name as official_office_name,
    tbl_hs_contacts.properties_candidate_office as candidate_office,
    tbl_hs_contacts.properties_office_level as office_level,
    tbl_hs_contacts.properties_office_type as office_type,
    tbl_hs_contacts.properties_party_affiliation as party_affiliation,
    tbl_hs_contacts.properties_partisan_type as is_partisan,

    -- Geographic information
    coalesce(
        tbl_states.state_cleaned_postal_code, tbl_hs_contacts.properties_state
    ) as state,
    tbl_hs_contacts.properties_city as city,
    tbl_hs_contacts.properties_candidate_district as district,
    tbl_hs_contacts.properties_open_seat as seat,
    try_cast(tbl_hs_contacts.properties_population as int) as population,

    -- Election dates
    tbl_hs_contacts.properties_filing_deadline as filing_deadline,
    tbl_hs_contacts.properties_primary_election_date as primary_election_date,
    coalesce(
        tbl_hs_contacts.properties_election_date,
        tbl_hs_contacts.properties_general_election_date
    ) as general_election_date,
    cast(null as date) as runoff_election_date,

    -- Election context
    tbl_hs_contacts.properties_incumbent as is_incumbent,
    tbl_hs_contacts.properties_uncontested as is_uncontested,
    tbl_hs_contacts.properties_number_opponents as number_of_opponents,

    -- Metadata
    tbl_hs_contacts.`createdAt` as created_at,
    tbl_hs_contacts.`updatedAt` as updated_at
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }} tbl_hs_contacts
left join
    {{ ref("clean_states") }} as tbl_states
    on trim(upper(tbl_hs_contacts.properties_state)) = tbl_states.state_raw
where
    1 = 1
    and tbl_hs_contacts.properties_firstname is not null
    and tbl_hs_contacts.properties_lastname is not null
    {% if is_incremental() %}
        and tbl_hs_contacts.`updatedAt` >= (select max(updated_at) from {{ this }})
    {% endif %}
