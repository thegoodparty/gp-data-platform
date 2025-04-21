with
    source as (select * from {{ source("airbyte_source", "gp_api_db_user") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("zip") }},
            {{ adapter.quote("name") }},
            {{ adapter.quote("email") }},
            {{ adapter.quote("phone") }},
            {{ adapter.quote("roles") }},
            {{ adapter.quote("avatar") }},
            {{ adapter.quote("password") }},
            {{ adapter.quote("last_name") }},
            {{ adapter.quote("meta_data") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("first_name") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("has_password") }},
            {{ adapter.quote("password_reset_token") }}

        from source
    )
select *
from renamed
