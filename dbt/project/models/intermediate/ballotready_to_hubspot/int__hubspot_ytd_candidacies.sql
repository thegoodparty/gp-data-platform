{{ config(tags=["intermediate", "hubspot"]) }}
-- pulling in current year results from hubspot
select
    id,
    {{
        generate_candidate_code(
            "properties_firstname",
            "properties_lastname",
            "properties_state",
            "properties_office_type",
            "properties_city",
        )
    }} as hubspot_candidate_code,
    properties_firstname,
    properties_lastname,
    properties_phone,
    properties_type,
    properties_product_user,
    properties_election_date,
    properties_state,
    properties_city,
    properties_office_type,
    properties_official_office_name,
    properties_number_of_seats_available,
    updated_at
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
where
    (properties_type like '%Self-Filer Lead%' or properties_product_user = 'yes')
    and year(properties_election_date) = year(current_date)
