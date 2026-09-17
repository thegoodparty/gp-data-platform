-- Parse of the frozen Airbyte raw stream for campaigns. The destination's
-- Direct-Load upgrade stopped writing this table, so it holds every state
-- extracted before then and nothing after. Not deduplicated.
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
            _airbyte_data:completed_task_ids::string as completed_task_ids,
            _airbyte_data:organization_slug::string as organization_slug
        from source
    )

select *
from parsed
