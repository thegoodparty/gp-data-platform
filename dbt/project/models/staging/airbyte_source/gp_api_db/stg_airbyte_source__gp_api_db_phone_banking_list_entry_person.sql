with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_phone_banking_list_entry_person") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("person_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("phone_banking_list_entry_id") }}

        from source
    )
select *
from renamed
