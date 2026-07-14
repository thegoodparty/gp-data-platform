with
    source as (
        select * from {{ source("airbyte_source", "gp_api_db_race_opponent_contrast") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            status,
            routing,
            issue_tag,
            edit_count,
            finding_id,
            source_url,
            campaign_id,
            opponent_fact,
            candidate_fact,
            routed_story_id,
            contrast_sentence,
            routed_website_id,
            routed_outreach_id,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at
        from source
    )
select *
from renamed
