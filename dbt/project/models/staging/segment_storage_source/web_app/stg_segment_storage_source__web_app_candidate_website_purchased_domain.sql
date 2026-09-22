select *
from
    {{ source("segment_storage_source_web_app", "candidate_website_purchased_domain") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
