select *
from {{ source("segment_storage_source_web_app", "sign_in_click_create_account") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
