-- Never-delete owners layer: Lakeflow returns current owners only, so an archived
-- owner would vanish, and this table is the only copy of the frozen ones.
--
-- full_refresh=false on purpose: a full refresh would rebuild from current state
-- and lose the archived owners permanently. The seed unions both producers,
-- Lakeflow winning; later runs read Lakeflow only, so a stale frozen row can
-- never overwrite a newer one.
--
-- The row's own `archived` flag is not the active/archived signal: owners staging
-- derives that from the live Lakeflow snapshot, so an owner archived after the
-- seed does not read as active forever.
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        full_refresh=false,
    )
}}

-- One normalized projection for every input: Airbyte carries teams as a JSON
-- string, Lakeflow as the already-parsed array.
{% set normalized_columns %}
    cast(null as string) as _airbyte_raw_id,
    cast(null as timestamp) as _airbyte_extracted_at,
    cast(null as string) as _airbyte_meta,
    cast(null as bigint) as _airbyte_generation_id,
    cast(id as int) as id,
    email,
    teams,
    cast(userId as bigint) as user_id,
    archived,
    lastName as last_name,
    to_timestamp(createdAt) as created_at,
    firstName as first_name,
    to_timestamp(updatedAt) as updated_at,
    cast(userIdIncludingInactive as int) as user_id_including_inactive
{% endset %}

{% if is_incremental() %}
    select {{ normalized_columns }} from {{ source("lakeflow_source", "owners") }}

{% else %}
    with
        lakeflow_owners as (
            select {{ normalized_columns }}
            from {{ source("lakeflow_source", "owners") }}
        ),

        airbyte_active as (
            select
                {{ adapter.quote("_airbyte_raw_id") }},
                {{ adapter.quote("_airbyte_extracted_at") }},
                {{ adapter.quote("_airbyte_meta") }},
                {{ adapter.quote("_airbyte_generation_id") }},
                cast({{ adapter.quote("id") }} as int) as id,
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
                cast(
                    {{ adapter.quote("userIdIncludingInactive") }} as int
                ) as user_id_including_inactive
            from {{ source("airbyte_source", "hubspot_api_owners") }}
        ),

        airbyte_archived as (
            select
                {{ adapter.quote("_airbyte_raw_id") }},
                {{ adapter.quote("_airbyte_extracted_at") }},
                {{ adapter.quote("_airbyte_meta") }},
                {{ adapter.quote("_airbyte_generation_id") }},
                cast({{ adapter.quote("id") }} as int) as id,
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
                cast(
                    {{ adapter.quote("userIdIncludingInactive") }} as int
                ) as user_id_including_inactive
            from {{ source("airbyte_source", "hubspot_api_owners_archived") }}
        ),

        -- Lakeflow, then Airbyte active, then Airbyte archived: the two frozen
        -- streams overlap on reactivated owners, so every id lands exactly once.
        seeded as (
            select *
            from lakeflow_owners
            union all
            select *
            from airbyte_active
            where id not in (select id from lakeflow_owners where id is not null)
            union all
            select *
            from airbyte_archived
            where
                id not in (select id from lakeflow_owners where id is not null)
                and id not in (select id from airbyte_active where id is not null)
        )

    select *
    from seeded
{% endif %}
