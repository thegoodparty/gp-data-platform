with
    source as (
        select * from {{ source("airbyte_source", "amplitude_api_events_list") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("value") }},
            {{ adapter.quote("hidden") }},
            {{ adapter.quote("totals") }},
            {{ adapter.quote("deleted") }},
            {{ adapter.quote("display") }},
            {{ adapter.quote("autohidden") }},
            {{ adapter.quote("non_active") }},
            {{ adapter.quote("flow_hidden") }},
            {{ adapter.quote("in_waitroom") }},
            {{ adapter.quote("totals_delta") }},
            {{ adapter.quote("clusters_hidden") }}

        from source
    )
select *
from renamed
