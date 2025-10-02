with
    source as (
        select *
        from {{ source("airbyte_source", "amplitude_api_average_session_length") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("length") }}

        from source
    )
select *
from renamed
