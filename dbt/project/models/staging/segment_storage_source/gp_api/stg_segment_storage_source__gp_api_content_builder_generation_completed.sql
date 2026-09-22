select *
from {{ source("segment_storage_source", "content_builder_generation_completed") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
