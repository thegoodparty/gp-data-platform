select * from {{ source("segment_storage_source_web_app", "question_complete") }}
