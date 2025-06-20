{{
  config(
    materialized='table',
    tags=['intermediate', 'candidacy']
  )
}}

WITH extracted_engagements AS (
  SELECT DISTINCT
    companies.companies_id_main,
    REGEXP_EXTRACT(engagements.associations_companyIds, '\\[(\\d+)\\]', 1) AS company_id_association,
    REGEXP_EXTRACT(engagements.associations_contactIds, '\\[(\\d+)\\]', 1) AS contact_id_association
  FROM {{ ref('int__hs_companies_recent') }} AS companies
  LEFT JOIN {{ ref('stg_airbyte_source__hubspot_api_engagements') }} AS engagements
    ON companies.companies_id_main = REGEXP_EXTRACT(engagements.associations_companyIds, '\\[(\\d+)\\]', 1)
  WHERE companies.companies_id_main IS NOT NULL
),

joined_data AS (
  SELECT 
    companies.*,
    contacts.id AS contact_id,
    contacts.properties_candidate_id_source AS candidate_id_source,
    contacts.properties_firstname AS first_name,
    contacts.properties_lastname AS last_name,
    contacts.properties_birth_date AS birth_date, 
    contacts.properties_instagram_handle AS instagram_handle,
    contacts.properties_population AS population,
    contacts.properties_candidate_id_tier AS candidate_id_tier,
    contacts.properties_email AS email_contacts,
    contacts.updatedAt AS contact_updated_at,

    -- Matching logic
    CASE 
      WHEN LOWER(contacts.properties_email) = LOWER(companies.email) THEN 1
      ELSE 0
    END AS email_match,

    CASE 
      WHEN LOWER(TRIM(companies.full_name)) = LOWER(TRIM(CONCAT(contacts.properties_firstname, ' ', contacts.properties_lastname))) THEN 1
      ELSE 0
    END AS name_match

  FROM {{ ref('int_companies_recent') }} AS companies
  LEFT JOIN extracted_engagements AS engagements
    ON companies.companies_id_main = engagements.companies_id_main
  LEFT JOIN {{ ref('stg_airbyte_source__hubspot_api_contacts') }} AS contacts
    ON contacts.id = engagements.contact_id_association
),

ranked_matches AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY companies_id_main
      ORDER BY 
        email_match DESC,
        name_match DESC,
        contact_updated_at DESC
    ) AS row_rank
  FROM joined_data
)

SELECT * 
FROM ranked_matches
WHERE row_rank = 1