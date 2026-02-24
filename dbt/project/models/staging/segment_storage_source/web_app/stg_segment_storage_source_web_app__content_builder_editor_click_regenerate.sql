{% set source_ref = source(
    "segment_storage_source_web_app",
    "content_builder_editor_click_regenerate",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
