with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_contact_interaction_door_knock") }}
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
            {{ adapter.quote("outcome") }},
            {{ adapter.quote("follow_up") }},
            {{ adapter.quote("person_id") }},
            {{ adapter.quote("source_id") }},
            {{ adapter.quote("will_vote") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("occurred_at") }},
            {{ adapter.quote("actor_user_id") }},
            {{ adapter.quote("support_answer") }},
            {{ adapter.quote("organization_slug") }}

        from source
    )
select *
from renamed
