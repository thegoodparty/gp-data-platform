with
    source as (
        select * from {{ source("airbyte_source", "amplitude_api_annotations") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("date") }},
            {{ adapter.quote("label") }},
            {{ adapter.quote("details") }}

        from source
    )
select *
from renamed
