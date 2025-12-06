{{
    config(
        tags=["intermediate", "hubspot", "phone_numbers"],
    )
}}

-- Extract unique phone numbers from HubSpot contacts for deduplication
select distinct
    properties_firstname as first_name,
    properties_lastname as last_name,
    trim(regexp_replace(properties_phone, '[^0-9]', '')) as phone
from {{ ref("int__hubspot_ytd_candidacies") }}
where
    properties_phone is not null
    and trim(regexp_replace(properties_phone, '[^0-9]', '')) != ''
