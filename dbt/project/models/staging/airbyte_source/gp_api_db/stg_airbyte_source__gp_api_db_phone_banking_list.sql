with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_phone_banking_list") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("script") }},
            {{ adapter.quote("purpose") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("sheet_count") }},
            {{ adapter.quote("organization_slug") }},
            {{ adapter.quote("voter_file_filter_id") }}

        from source
    )
select *
from renamed
