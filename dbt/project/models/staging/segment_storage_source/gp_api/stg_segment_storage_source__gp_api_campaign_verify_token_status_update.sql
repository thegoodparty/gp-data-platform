select *
from {{ source("segment_storage_source", "campaign_verify_token_status_update") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
