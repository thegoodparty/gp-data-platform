with
    source as (select * from {{ source("airbyte_source", "gp_api_db_campaign") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("data") }},
            {{ adapter.quote("slug") }},
            {{ adapter.quote("tier") }},
            {{ adapter.quote("is_pro") }},
            {{ adapter.quote("details") }},
            {{ adapter.quote("did_win") }},
            {{ adapter.quote("is_demo") }},
            {{ adapter.quote("user_id") }},
            {{ adapter.quote("is_active") }},
            {{ adapter.quote("ai_content") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("is_verified") }},
            {{ adapter.quote("date_verified") }},
            {{ adapter.quote("vendor_ts_data") }},
            {{ adapter.quote("completed_task_ids") }}

        from source
    )
select *
from renamed
