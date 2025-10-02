with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_ecanvasser_contact") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("type") }},
            {{ adapter.quote("donor") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("gender") }},
            {{ adapter.quote("deceased") }},
            {{ adapter.quote("house_id") }},
            {{ adapter.quote("action_id") }},
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("volunteer") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("created_by") }},
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("home_phone") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("mobile_phone") }},
            {{ adapter.quote("organization") }},
            {{ adapter.quote("date_of_birth") }},
            {{ adapter.quote("ecanvasser_id") }},
            {{ adapter.quote("year_of_birth") }},
            {{ adapter.quote("ecanvasserHouseId") }},
            {{ adapter.quote("unique_identifier") }},
            {{ adapter.quote("last_interaction_id") }}

        from source
    )
select *
from renamed
