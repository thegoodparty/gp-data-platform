select *
from {{ source("segment_storage_source_web_app", "users") }}
where {{ dsar_not_suppressed("id", "gp_api_user_id") }}
