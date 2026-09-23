select *
from {{ source("segment_storage_source_web_app", "campaign_assistant_chatbot_input") }}
