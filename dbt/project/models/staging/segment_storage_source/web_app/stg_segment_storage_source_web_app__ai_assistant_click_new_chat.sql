{% set source_ref = source(
    "segment_storage_source_web_app", "ai_assistant_click_new_chat"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
