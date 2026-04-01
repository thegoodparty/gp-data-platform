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
            {{ adapter.quote("content") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("elected_office_id") }},
            {{ adapter.quote("is_opt_out") }},
            {{ adapter.quote("person_cell_phone") }},
            {{ adapter.quote("person_id") }},
            {{ adapter.quote("poll_id") }},
            {{ adapter.quote("sender") }},
            {{ adapter.quote("sent_at") }},
            {{ adapter.quote("updated_at") }}

        from source
    )
select *
from renamed
