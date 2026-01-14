with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_poll_individual_message") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("poll_id") }},
            {{ adapter.quote("sent_at") }},
            {{ adapter.quote("person_id") }}

        from source
    )
select *
from renamed
