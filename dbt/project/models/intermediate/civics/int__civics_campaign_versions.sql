-- One row per campaign version. The product DB overwrites a campaign in
-- place when a user reuses it for a new election, so current state alone
-- loses the earlier run. Versions are detected by changes in election
-- context (election date, position, office, state).
--
-- History comes from Airbyte's insert-only raw stream, which recorded every
-- overwrite until the Databricks destination moved to Direct-Load (4.0.0)
-- and stopped writing raw tables. That table is frozen history. Current
-- state comes from the live final table, so the latest version of every
-- campaign stays fresh no matter what happens to the raw table.
with
    frozen_history as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            _airbyte_data:id::bigint as id,
            _airbyte_data:data::string as data,
            _airbyte_data:slug::string as slug,
            _airbyte_data:tier::string as tier,
            _airbyte_data:is_pro::boolean as is_pro,
            _airbyte_data:details::string as details,
            _airbyte_data:did_win::boolean as did_win,
            _airbyte_data:is_demo::boolean as is_demo,
            _airbyte_data:user_id::bigint as user_id,
            _airbyte_data:is_active::boolean as is_active,
            _airbyte_data:ai_content::string as ai_content,
            _airbyte_data:created_at::timestamp as created_at,
            _airbyte_data:updated_at::timestamp as updated_at,
            _airbyte_data:is_verified::boolean as is_verified,
            _airbyte_data:date_verified::timestamp as date_verified,
            _airbyte_data:vendor_ts_data::string as vendor_ts_data,
            _airbyte_data:completed_task_ids::string as completed_task_ids,
            _airbyte_data:organization_slug::string as organization_slug,
            -- The same record lands in both tables with the same extracted_at;
            -- ties resolve to the live table.
            1 as _source_rank
        from
            {{
                source(
                    "airbyte_internal",
                    "airbyte_source_raw__stream_gp_api_db_campaign",
                )
            }}
    ),

    -- Direct-Load types these timestamps without zone; the raw parse above
    -- yields TIMESTAMP, so cast to keep the union and the mart's types as-is.
    current_state as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            data,
            slug,
            tier,
            is_pro,
            details,
            did_win,
            is_demo,
            user_id,
            is_active,
            ai_content,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at,
            is_verified,
            cast(date_verified as timestamp) as date_verified,
            vendor_ts_data,
            completed_task_ids,
            organization_slug,
            0 as _source_rank
        from {{ ref("stg_airbyte_source__gp_api_db_campaign") }}
    ),

    unioned as (
        select *
        from frozen_history
        union all
        select *
        from current_state
    ),

    -- Same fingerprint as the retired raw-stream model, so version ids are
    -- stable across the swap.
    versioned as (
        select
            *,
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "id",
                        "details:electiondate::string",
                        "details:positionid::string",
                        "details:office::string",
                        "details:state::string",
                    ]
                )
            }} as campaign_version_id
        from unioned
    ),

    deduplicated as (
        select *
        from versioned
        qualify
            row_number() over (
                partition by campaign_version_id
                order by _airbyte_extracted_at desc, _source_rank
            )
            = 1
    )

select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    _airbyte_generation_id,
    campaign_version_id,
    id,
    data,
    slug,
    tier,
    is_pro,
    details,
    did_win,
    is_demo,
    user_id,
    is_active,
    ai_content,
    created_at,
    updated_at,
    is_verified,
    date_verified,
    vendor_ts_data,
    completed_task_ids,
    organization_slug
from deduplicated
