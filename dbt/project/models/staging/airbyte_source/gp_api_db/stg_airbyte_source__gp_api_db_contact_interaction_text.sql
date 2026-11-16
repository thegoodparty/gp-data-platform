with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_contact_interaction_text") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("note") }},
            {{ adapter.quote("manual") }},
            {{ adapter.quote("person_id") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("occurred_at") }},
            {{ adapter.quote("outreach_id") }},
            {{ adapter.quote("opted_out_at") }},
            {{ adapter.quote("responded_at") }},
            {{ adapter.quote("source_event_id") }},
            {{ adapter.quote("organization_slug") }}

        from source
    )
select *
from renamed
