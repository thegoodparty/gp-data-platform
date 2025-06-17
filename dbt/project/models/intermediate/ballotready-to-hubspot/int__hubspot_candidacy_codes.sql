{{
    config(
        auto_liquid_cluster=true,
        tags=["intermediate", "hubspot"]
    )
}}

-- get the unique candidacies from the hubspot data 
SELECT 
    {{ generate_candidate_code('properties_firstname', 'properties_lastname', 'properties_state', 'properties_office_type') }} AS hs_candidate_code
FROM {{ ref('int__hubspot_ytd_candidacies') }}
WHERE 
  properties_firstname is not null AND
  properties_lastname is not null AND
  properties_state is not null AND
  properties_office_type is not null