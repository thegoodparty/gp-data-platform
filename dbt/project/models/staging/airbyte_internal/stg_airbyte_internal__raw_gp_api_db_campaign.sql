-- Parses the insert-only raw Airbyte stream for campaigns and deduplicates
-- to one row per distinct campaign version. When a user reuses a campaign
-- for a new election, the product DB overwrites the row in place. This model
-- recovers historical versions by detecting changes in election context fields
-- (election date, position, office, state) across Airbyte sync snapshots.
with
    source as (
        select *
        from
            {{
                source(
                    "airbyte_internal",
                    "airbyte_source_raw__stream_gp_api_db_campaign",
                )
            }}
    ),

    parsed as (
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
            _airbyte_data:completed_task_ids::string as completed_task_ids
        from source
    ),

    -- Create a fingerprint based on election context fields. A change in any
    -- of these indicates the user started a new campaign that overwrote the
    -- previous one in the product DB.
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
        from parsed
    ),

    -- Keep the latest snapshot for each distinct campaign version
    deduplicated as (
        select *
        from versioned
        qualify
            row_number() over (
                partition by campaign_version_id order by _airbyte_extracted_at desc
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
    completed_task_ids
from deduplicated
