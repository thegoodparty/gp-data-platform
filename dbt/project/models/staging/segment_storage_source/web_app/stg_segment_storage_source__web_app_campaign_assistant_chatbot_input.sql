{% set source_ref = source(
    "segment_storage_source_web_app", "campaign_assistant_chatbot_input"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
