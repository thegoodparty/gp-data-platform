select
    -- identifiers
    id,
    contacts,

    -- candidate information
    properties_candidate_name as candidate_name,
    properties_candidate_email as candidate_email,
    properties_candidate_office as candidate_office,
    properties_candidate_party as candidate_party,
    properties_candidate_district as candidate_district,
    cast(properties_candidates_seats as int) as candidates_seats,
    properties_verified_candidates as verified_candidates,
    properties_pledge_status as pledge_status,
    properties_incumbent as incumbent,

    -- office information
    properties_official_office_name as official_office_name,
    properties_office_level as office_level,
    properties_office_type as office_type,
    properties_partisan_np as partisan_np,
    properties_open_seat_ as open_seat,

    -- contact information
    properties_phone as phone,
    properties_website as website,
    properties_linkedin_company_page as linkedin_company_page,
    properties_twitterhandle as twitter_handle,
    properties_facebook_url as facebook_url,
    properties_address as address,

    -- geographic information
    properties_state as state,
    properties_city as city,

    -- election dates
    properties_filing_deadline as filing_deadline,
    properties_primary_date as primary_date,
    properties_election_date as election_date,
    properties_runoff_date as runoff_date,

    -- election context
    properties_uncontested as uncontested,
    properties_number_of_opponents as number_of_opponents,
    cast(properties_seats_available as int) as seats_available,

    -- election results
    properties_general_election_result as general_election_result,
    cast(properties_general_votes_received as int) as general_votes_received,
    cast(properties_total_general_votes_cast as int) as total_general_votes_cast,

    -- assessments
    properties_viability_2_0 as viability_score,

    -- metadata
    createdat,
    updatedat
from {{ source("airbyte_source", "hubspot_api_companies") }}
