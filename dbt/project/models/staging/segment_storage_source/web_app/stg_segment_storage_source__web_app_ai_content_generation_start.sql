{% set source_ref = source(
    "segment_storage_source_web_app", "ai_content_generation_start"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
