select *
from {{ source("segment_storage_source_web_app", "polls_poll_question_viewed") }}
