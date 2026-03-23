select
    -- identifiers
    id,
    companies,

    -- personal information
    properties_full_name,
    properties_firstname,
    properties_lastname,
    properties_birth_date,
    properties_email,
    properties_phone,
    properties_website,
    properties_instagram_handle,
    properties_linkedin_url,
    properties_twitterhandle,
    properties_facebook_url,
    properties_address,

    -- candidate information
    properties_candidate_id_source,
    properties_candidate_id_tier,
    properties_pledge_status,
    properties_verified_candidate_status,
    properties_type,
    properties_product_user,

    -- office information
    properties_official_office_name,
    properties_candidate_office,
    properties_office_level,
    properties_office_type,
    properties_party_affiliation,
    properties_partisan_type,

    -- geographic information
    properties_state,
    properties_city,
    properties_candidate_district,
    properties_open_seat,
    properties_population,

    -- election dates
    properties_filing_deadline,
    properties_primary_election_date,
    properties_election_date,
    properties_general_election_date,
    properties_start_date,

    -- election context
    properties_uncontested,
    properties_number_opponents,
    properties_number_of_seats_available,

    -- metadata
    createdat as created_at,
    updatedat as updated_at
from {{ source("airbyte_source", "hubspot_api_contacts") }}
