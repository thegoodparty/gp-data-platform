select *
from {{ source("segment_storage_source_web_app", "ai_assistant_ask_a_question") }}
