{% set source_ref = source(
    "segment_storage_source_web_app", "content_builder_click_generate"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
