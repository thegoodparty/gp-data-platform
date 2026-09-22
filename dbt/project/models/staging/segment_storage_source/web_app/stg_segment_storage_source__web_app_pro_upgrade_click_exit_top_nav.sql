select *
from {{ source("segment_storage_source_web_app", "pro_upgrade_click_exit_top_nav") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
