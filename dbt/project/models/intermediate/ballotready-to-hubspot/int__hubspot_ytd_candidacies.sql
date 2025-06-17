{{
    config(
        tags=["intermediate", "hubspot"]
    )
}}
-- pulling in current year results from hubspot

SELECT *
FROM dbt.stg_airbyte_source__hubspot_api_contacts t1
WHERE (
    properties_type LIKE '%Self-Filer Lead%'
    OR properties_product_user = 'yes'
)
AND properties_election_date BETWEEN DATE_TRUNC('year', CURRENT_DATE) AND DATE_TRUNC('year', CURRENT_DATE + INTERVAL 1 YEAR) - INTERVAL 1 DAY
