with
    source as (
        select * from {{ source("airbyte_source", "hubspot_api_owners_archived") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            int({{ adapter.quote("id") }}) as id,
            {{ adapter.quote("email") }},
            from_json(
                {{ adapter.quote("teams") }},
                'array<struct<id:string,name:string,primary:boolean>>'
            ) as teams,
            {{ adapter.quote("userId") }} as user_id,
            {{ adapter.quote("archived") }},
            {{ adapter.quote("lastName") }} as last_name,
            to_timestamp({{ adapter.quote("createdAt") }}) as created_at,
            {{ adapter.quote("firstName") }} as first_name,
            to_timestamp({{ adapter.quote("updatedAt") }}) as updated_at,
            int(
                {{ adapter.quote("userIdIncludingInactive") }}
            ) as user_id_including_inactive

        from source
    )
select *
from renamed
