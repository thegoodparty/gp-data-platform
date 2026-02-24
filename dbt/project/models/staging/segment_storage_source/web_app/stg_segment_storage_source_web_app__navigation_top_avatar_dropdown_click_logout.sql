{% set source_ref = source(
    "segment_storage_source_web_app",
    "navigation_top_avatar_dropdown_click_logout",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
