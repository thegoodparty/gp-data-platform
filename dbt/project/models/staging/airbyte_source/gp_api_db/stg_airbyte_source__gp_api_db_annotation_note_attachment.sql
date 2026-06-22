with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_annotation_note_attachment") }}
    ),
    renamed as (
        select
            {{ adapter.quote("_airbyte_raw_id") }},
            {{ adapter.quote("_airbyte_extracted_at") }},
            {{ adapter.quote("_airbyte_meta") }},
            {{ adapter.quote("_airbyte_generation_id") }},
            {{ adapter.quote("id") }},
            {{ adapter.quote("note_id") }},
            {{ adapter.quote("ocr_text") }},
            {{ adapter.quote("file_name") }},
            {{ adapter.quote("mime_type") }},
            {{ adapter.quote("ocr_error") }},
            {{ adapter.quote("created_at") }},
            {{ adapter.quote("ocr_status") }},
            {{ adapter.quote("size_bytes") }},
            {{ adapter.quote("storage_key") }},
            {{ adapter.quote("ocr_completed_at") }}

        from source
    )
select *
from renamed
