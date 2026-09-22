select *
from {{ source("segment_storage_source_web_app", "polls_create_poll_clicked") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
