with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_tcr_compliance") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("ein") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("status") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("filing_url") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("campaign_id") }},
            {{ adapter.quote("tdlc_number") }},
            {{ adapter.quote("postalAddress") }},
            {{ adapter.quote("committee_name") }},
            {{ adapter.quote("website_domain") }},
            {{ adapter.quote("peerly_identity_id") }},
            {{ adapter.quote("matching_contact_fields") }},
            {{ adapter.quote("peerly_registration_link") }},
            {{ adapter.quote("peerly_identity_profile_link") }},
            {{ adapter.quote("peerly_10dlc_brand_submission_key") }}

        from source
    )
select *
from renamed
