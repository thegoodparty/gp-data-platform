{% set source_ref = source(
    "segment_storage_source_web_app", "navigation_dashboard_click_my_profile"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
