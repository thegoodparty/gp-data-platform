select *
from {{ source("segment_storage_source_web_app", "profile_office_details_click_edit") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
