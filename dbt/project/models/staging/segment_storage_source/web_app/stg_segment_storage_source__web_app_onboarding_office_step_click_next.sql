select *
from {{ source("segment_storage_source_web_app", "onboarding_office_step_click_next") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
