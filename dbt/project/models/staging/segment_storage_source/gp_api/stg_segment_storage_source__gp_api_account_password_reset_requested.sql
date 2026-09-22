select *
from {{ source("segment_storage_source", "account_password_reset_requested") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
