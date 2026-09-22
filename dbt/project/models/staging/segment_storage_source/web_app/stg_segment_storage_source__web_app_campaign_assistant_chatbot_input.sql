select *
from {{ source("segment_storage_source_web_app", "campaign_assistant_chatbot_input") }}
where {{ dsar_not_suppressed("user_id", "gp_api_user_id") }}
