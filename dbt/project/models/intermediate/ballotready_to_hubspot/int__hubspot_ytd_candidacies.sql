{{ config(tags=["intermediate", "hubspot"]) }}
-- pulling in current year results from hubspot
select
    id,
    {{
        generate_candidate_code(
            "first_name",
            "last_name",
            "state",
            "office_type",
            "city",
        )
    }} as hubspot_candidate_code,
    first_name,
    last_name,
    phone,
    type,
    product_user,
    election_date,
    state,
    city,
    office_type,
    official_office_name,
    number_of_seats_available,
    updated_at
from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
where
    (type like '%Self-Filer Lead%' or product_user = 'yes')
    and year(election_date) = year(current_date)
