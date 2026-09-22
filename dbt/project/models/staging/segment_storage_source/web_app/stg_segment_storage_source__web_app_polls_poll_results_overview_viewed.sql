select *
from
    {{ source("segment_storage_source_web_app", "polls_poll_results_overview_viewed") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
