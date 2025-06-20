{{
  config(
    materialized='view',
    tags=['intermediate', 'candidacy']
  )
}}

-- Recent candidates (companies) from HubSpot data with standardized fields
SELECT 
  'gp_candidacy_id-tbd' AS gp_candidacy_id,
  'candidacy_id-tbd' AS candidacy_id,
  'gp_user_id-tbd' AS gp_user_id,
  'gp_contest_id-tbd' AS gp_contest_id,
  id AS companies_id_main,

  -- Personal information
  properties_candidate_name AS full_name,
  properties_candidate_email AS email,
  properties_phone AS phone_number,
  properties_website AS website_url,
  properties_linkedin_company_page AS linkedin_url,
  properties_twitterhandle AS twitter_handle,
  properties_facebook_url AS facebook_url,
  properties_address AS street_address,

  -- Office information
  properties_official_office_name AS official_office_name,
  properties_candidate_office AS candidate_office, 
  properties_office_level AS office_level,
  properties_office_type AS office_type,
  properties_candidate_party AS party_affiliation,
  properties_partisan_np AS is_partisan,

  -- Geographic information
  properties_state AS state,
  properties_city AS city,
  properties_candidate_district AS district,
  properties_candidates_seats AS seat,

  -- Election dates
  properties_filing_deadline AS filing_deadline,
  properties_primary_date AS primary_election_date,
  properties_election_date AS general_election_date,
  properties_runoff_date AS runoff_election_date,

  -- Election context
  properties_incumbent AS is_incumbent,
  properties_uncontested AS is_uncontested, 
  properties_number_of_opponents AS number_of_opponents,
  properties_open_seat_ AS is_open_seat,
  properties_general_election_result AS candidacy_result,

  -- Metadata
  createdAt AS created_at,
  updatedAt AS updated_at

-- FROM {{ ref('stg_airbyte_source__hubspot_api_companies') }}
FROM goodparty_data_catalog.dbt.stg_airbyte_source__hubspot_api_companies
WHERE updatedAt >= {{ dbt.current_timestamp() }} - INTERVAL 7 DAYS