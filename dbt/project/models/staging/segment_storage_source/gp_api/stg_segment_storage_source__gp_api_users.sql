select *
from {{ source("segment_storage_source", "users") }}
where {{ dsar_not_suppressed("id", "gp_api_user_id") }}
