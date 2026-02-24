{% set source_ref = source(
    "segment_storage_source_web_app", "navigation_top_click_avatar_dropdown"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
