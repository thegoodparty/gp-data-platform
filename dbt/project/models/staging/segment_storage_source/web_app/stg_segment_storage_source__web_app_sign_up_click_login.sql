select *
from {{ source("segment_storage_source_web_app", "sign_up_click_login") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
