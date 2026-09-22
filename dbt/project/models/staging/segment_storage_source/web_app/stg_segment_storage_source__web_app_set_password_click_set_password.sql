select *
from {{ source("segment_storage_source_web_app", "set_password_click_set_password") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
