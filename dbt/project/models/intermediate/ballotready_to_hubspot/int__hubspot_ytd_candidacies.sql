{{ config(tags=["intermediate", "hubspot"]) }}
-- pulling in current year results from hubspot
select
    id,
    properties_firstname,
    properties_lastname,
    properties_phone,
    properties_type,
    properties_product_user,
    properties_election_date,
    properties_state,
    properties_office_type,
    properties_official_office_name,
    properties_number_of_seats_available,
    {{ adapter.quote("updatedAt") }} as updated_at
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
where
    (properties_type like '%Self-Filer Lead%' or properties_product_user = 'yes')
    and properties_election_date
    between date_trunc('year', current_date) and date_trunc(
        'year', current_date + interval 1 year
    )
    - interval 1 day
