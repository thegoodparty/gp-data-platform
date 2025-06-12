-- pulling in current year results 
SELECT *
FROM {{ ref('stg_airbyte_source__hubspot_api_contacts') }}
WHERE (
    properties_type LIKE '%Self-Filer Lead%'
    OR properties_product_user = 'yes'
)
AND properties_election_date BETWEEN DATE_TRUNC('year', CURRENT_DATE) AND DATE_TRUNC('year', CURRENT_DATE + INTERVAL 1 YEAR) - INTERVAL 1 DAY
