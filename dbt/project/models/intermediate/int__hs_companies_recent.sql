{{
    config(
        materialized="view",
        tags=["intermediate", "candidacy", "companies", "hubspot"],
    )
}}

-- Recent candidates (companies) from HubSpot data with standardized fields
select
    -- 'gp_candidacy_id-tbd' as gp_candidacy_id, -- need first_name, so add in
    -- downstream table
    'candidacy_id-tbd' as candidacy_id,
    'gp_user_id-tbd' as gp_user_id,
    'gp_contest_id-tbd' as gp_contest_id,
    id as companies_id_main,

    -- Personal information
    properties_candidate_name as full_name,
    properties_candidate_email as email,
    properties_phone as phone_number,
    properties_website as website_url,
    properties_linkedin_company_page as linkedin_url,
    properties_twitterhandle as twitter_handle,
    properties_facebook_url as facebook_url,
    properties_address as street_address,

    -- Office information
    properties_official_office_name as official_office_name,
    properties_candidate_office as candidate_office,
    properties_office_level as office_level,
    properties_office_type as office_type,
    properties_candidate_party as party_affiliation,
    properties_partisan_np as is_partisan,

    -- Geographic information
    properties_state as state,
    properties_city as city,
    properties_candidate_district as district,
    properties_candidates_seats as seat,

    -- Election dates
    properties_filing_deadline as filing_deadline,
    properties_primary_date as primary_election_date,
    properties_election_date as general_election_date,
    properties_runoff_date as runoff_election_date,

    -- Election context
    properties_incumbent as is_incumbent,
    properties_uncontested as is_uncontested,
    properties_number_of_opponents as number_of_opponents,
    properties_open_seat_ as is_open_seat,
    properties_general_election_result as candidacy_result,

    -- Metadata
    createdat as created_at,
    updatedat as updated_at

from {{ ref("stg_airbyte_source__hubspot_api_companies") }}
where updatedat >= {{ dbt.current_timestamp() }} - interval 7 days
