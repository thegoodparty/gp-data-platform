-- Never-delete owners layer. Lakeflow mirrors HubSpot's current state and returns
-- no archived owners at all, so an owner archived after the seed would simply
-- vanish; this table keeps it, which is the only way hubspot_owner_teams can name
-- the owner on a historical contact.
--
-- full_refresh=false is not decoration: the frozen Airbyte tables are the only
-- copy of the 113 archived owners, so a --full-refresh reaching this model would
-- rebuild it from current state and lose them permanently.
--
-- The seed reads both producers, Lakeflow winning; every later run reads Lakeflow
-- only. Reading the frozen tables incrementally would let a stale row overwrite a
-- newer one, the same rollback the contacts layer guards against.
--
-- The owner's own `archived` flag is whatever it last saw, and it is not the
-- active/archived signal downstream: hubspot_owner_teams derives that from the
-- live Lakeflow snapshot, so an owner archived after the seed does not read as
-- active forever.
{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id",
        full_refresh=false,
    )
}}

-- One normalized projection, used by every input. Airbyte's owners carry teams as
-- a JSON string and Lakeflow's as the already-parsed array Lakeflow hands back.
-- int()/bigint() match the types the staging models publish today.
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

        -- Precedence: Lakeflow, then Airbyte's active stream, then its archived
        -- stream, so every id lands exactly once. The two Airbyte streams overlap
        -- on reactivated owners, and the active row is the current state.
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
