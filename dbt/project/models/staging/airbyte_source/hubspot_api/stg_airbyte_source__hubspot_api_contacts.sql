select
    -- identifiers
    id,
    companies,

    -- personal information
    properties_full_name as full_name,
    properties_firstname as first_name,
    properties_lastname as last_name,
    properties_birth_date as birth_date,
    properties_email as email,
    properties_phone as phone,
    properties_website as website,
    properties_instagram_handle as instagram_handle,
    properties_linkedin_url as linkedin_url,
    properties_twitterhandle as twitter_handle,
    properties_facebook_url as facebook_url,
    properties_address as address,

    -- candidate information
    properties_candidate_id_source as candidate_id_source,
    properties_candidate_id_tier as candidate_id_tier,
    properties_pledge_status as pledge_status,
    properties_verified_candidate_status as verified_candidate_status,
    properties_type as type,
    properties_product_user as product_user,

    -- office information
    properties_official_office_name as official_office_name,
    properties_candidate_office as candidate_office,
    properties_office_level as office_level,
    properties_office_type as office_type,
    properties_party_affiliation as party_affiliation,
    properties_partisan_type as partisan_type,

    -- geographic information
    properties_state as state,
    properties_city as city,
    properties_candidate_district as candidate_district,
    properties_open_seat as open_seat,
    cast(properties_population as int) as population,

    -- election dates
    properties_filing_deadline as filing_deadline,
    properties_primary_election_date as primary_election_date,
    properties_election_date as election_date,
    properties_general_election_date as general_election_date,
    properties_start_date as start_date,

    -- election context
    properties_incumbent as incumbent,
    properties_uncontested as uncontested,
    properties_number_opponents as number_opponents,
    cast(cast(properties_number_of_seats_available as float) as int) as number_of_seats_available,

    -- metadata
    createdat as created_at,
    updatedat as updated_at
from {{ source("airbyte_source", "hubspot_api_contacts") }}
