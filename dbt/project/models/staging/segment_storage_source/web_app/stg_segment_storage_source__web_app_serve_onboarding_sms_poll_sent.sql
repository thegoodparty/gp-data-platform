select *
from {{ source("segment_storage_source_web_app", "serve_onboarding_sms_poll_sent") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
