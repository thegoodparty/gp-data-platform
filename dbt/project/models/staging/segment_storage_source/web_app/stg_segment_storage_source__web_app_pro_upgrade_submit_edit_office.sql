select *
from {{ source("segment_storage_source_web_app", "pro_upgrade_submit_edit_office") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
