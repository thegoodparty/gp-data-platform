with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_phone_banking_list_entry") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("seq") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("sheet_index") }},
            {{ adapter.quote("phone_banking_list_id") }}

        from source
    )
select *
from renamed
