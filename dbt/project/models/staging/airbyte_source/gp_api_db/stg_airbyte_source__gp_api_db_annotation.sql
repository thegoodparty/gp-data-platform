with
    source as (select * from {{ source("airbyte_source", "gp_api_db_annotation") }}),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("end") }},
            {{ adapter.quote("kind") }},
            {{ adapter.quote("start") }},
            {{ adapter.quote("note_id") }},
            {{ adapter.quote("json_path") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("updated_at") }},
            {{ adapter.quote("resource_id") }},
            {{ adapter.quote("resource_type") }},
            {{ adapter.quote("author_user_id") }},
            {{ adapter.quote("annotation_review_id") }},
            {{ adapter.quote("chat_conversation_id") }},
            {{ adapter.quote("annotation_bug_report_id") }}

        from source
    )
select *
from renamed
