select *
from {{ source("segment_storage_source", "tracks") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
