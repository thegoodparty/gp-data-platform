select *
from {{ source("segment_storage_source_web_app", "polls_create_poll_clicked") }}
