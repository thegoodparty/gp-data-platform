with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_contact_status_event") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("field") }},
            {{ adapter.quote("source") }},
            {{ adapter.quote("to_value") }},
            {{ adapter.quote("person_id") }},
            {{ adapter.quote("source_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("from_value") }},
            {{ adapter.quote("actor_user_id") }},
            {{ adapter.quote("organization_slug") }}

        from source
    )
select *
from renamed
