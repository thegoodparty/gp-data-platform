-- One row per campaign version. The product DB overwrites a campaign in
-- place when a user reuses it for a new election, so current state alone
-- loses the earlier run. History is the archived raw stream; current state is
-- the live table, so the latest version stays fresh whatever happens to the
-- archive.
with
    frozen_history as (
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
            created_at,
            updated_at,
            is_verified,
            date_verified,
            vendor_ts_data,
            completed_task_ids,
            organization_slug,
            -- The same record sits in both tables with the same extracted_at;
            -- ties go to the live table.
            1 as _source_rank
        from {{ ref("stg_archives__raw_gp_api_db_campaign") }}
    ),

    -- The live table stores these three timestamps without time zone while
    -- the raw parse yields TIMESTAMP. Cast here rather than in staging so the
    -- live model's other consumers keep their types until they are checked.
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

    -- These five fields define a version; changing them re-mints every
    -- campaign_version_id downstream.
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
