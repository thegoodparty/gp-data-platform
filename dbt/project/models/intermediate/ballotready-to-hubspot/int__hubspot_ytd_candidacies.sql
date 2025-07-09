{{ config(tags=["intermediate", "hubspot"]) }}
-- pulling in current year results from hubspot
select *
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
where
    (properties_type like '%Self-Filer Lead%' or properties_product_user = 'yes')
    and properties_election_date
    between date_trunc('year', current_date) and date_trunc(
        'year', current_date + interval 1 year
    )
    - interval 1 day
