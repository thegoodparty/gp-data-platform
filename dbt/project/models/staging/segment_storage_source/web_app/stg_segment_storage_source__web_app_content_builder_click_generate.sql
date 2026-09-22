select *
from {{ source("segment_storage_source_web_app", "content_builder_click_generate") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
