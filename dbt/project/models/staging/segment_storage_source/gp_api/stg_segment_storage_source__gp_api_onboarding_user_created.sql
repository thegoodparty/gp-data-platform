select *
from {{ source("segment_storage_source", "onboarding_user_created") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
