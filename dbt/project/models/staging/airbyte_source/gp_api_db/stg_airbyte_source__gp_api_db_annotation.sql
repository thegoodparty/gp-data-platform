with
    source as (select * from {{ source("airbyte_source", "gp_api_db_annotation") }}),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            cast(`start` as int) as `start`,
            cast(`end` as int) as `end`,
            kind,
            note_id,
            json_path,
            cast(created_at as timestamp) as created_at,
            cast(updated_at as timestamp) as updated_at,
            resource_id,
            resource_type,
            cast(author_user_id as int) as author_user_id,
            annotation_review_id,
            chat_conversation_id,
            annotation_bug_report_id
        from source
    )
select *
from renamed
