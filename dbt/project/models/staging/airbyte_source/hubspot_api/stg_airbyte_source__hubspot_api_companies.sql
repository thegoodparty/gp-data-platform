select
    -- identifiers
    id,
    contacts,

    -- candidate information
    properties_candidate_name,
    properties_candidate_email,
    properties_candidate_office,
    properties_candidate_party,
    properties_candidate_district,
    properties_candidates_seats,
    properties_verified_candidates,
    properties_pledge_status,
    properties_incumbent,

    -- office information
    properties_official_office_name,
    properties_office_level,
    properties_office_type,
    properties_partisan_np,
    properties_open_seat_,

    -- contact information
    properties_phone,
    properties_website,
    properties_linkedin_company_page,
    properties_twitterhandle,
    properties_facebook_url,
    properties_address,

    -- geographic information
    properties_state,
    properties_city,

    -- election dates
    properties_filing_deadline,
    properties_primary_date,
    properties_election_date,
    properties_runoff_date,

    -- election context
    properties_uncontested,
    properties_number_of_opponents,
    properties_seats_available,

    -- election results
    properties_general_election_result,
    properties_general_votes_received,
    properties_total_general_votes_cast,

    -- assessments
    properties_viability_2_0,

    -- metadata
    createdat,
    updatedat
from {{ source("airbyte_source", "hubspot_api_companies") }}
