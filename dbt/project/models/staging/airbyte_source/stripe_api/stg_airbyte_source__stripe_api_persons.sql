with
    source as (select * from {{ source("airbyte_source", "stripe_api_persons") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("dob") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("gender") }},
            {{ adapter.quote("object") }},
            {{ adapter.quote("account") }},
            {{ adapter.quote("address") }},
            {{ adapter.quote("created") }},
            {{ adapter.quote("updated") }},
            {{ adapter.quote("metadata") }},
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("is_deleted") }},
            {{ adapter.quote("nationality") }},
            {{ adapter.quote("address_kana") }},
            {{ adapter.quote("relationship") }},
            {{ adapter.quote("requirements") }},
            {{ adapter.quote("verification") }},
            {{ adapter.quote("address_kanji") }},
            {{ adapter.quote("first_name_kana") }},
            {{ adapter.quote("first_name_kanji") }},
            {{ adapter.quote("full_name_aliases") }},
            {{ adapter.quote("id_number_provided") }},
            {{ adapter.quote("political_exposure") }},
            {{ adapter.quote("registered_address") }},
            {{ adapter.quote("future_requirements") }},
            {{ adapter.quote("ssn_last_4_provided") }},
            {{ adapter.quote("id_number_secondary_provided") }}

        from source
    )
select *
from renamed
