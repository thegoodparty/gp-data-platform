select *
from {{ source("segment_storage_source_web_app", "ai_content_generation_start") }}
