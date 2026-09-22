select *
from {{ source("segment_storage_source_web_app", "pro_upgrade_click_go_to_stripe") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
