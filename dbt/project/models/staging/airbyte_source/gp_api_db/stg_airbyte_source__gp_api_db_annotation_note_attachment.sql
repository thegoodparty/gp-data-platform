with
    source as (
        select *
        from {{ source("airbyte_source", "gp_api_db_annotation_note_attachment") }}
    ),
    renamed as (
        select
            _airbyte_raw_id,
            _airbyte_extracted_at,
            _airbyte_meta,
            _airbyte_generation_id,
            id,
            note_id,
            ocr_text,
            file_name,
            mime_type,
            ocr_error,
            ocr_status,
            storage_key,
            cast(size_bytes as int) as size_bytes,
            cast(created_at as timestamp) as created_at,
            cast(ocr_completed_at as timestamp) as ocr_completed_at
        from source
    )
select *
from renamed
