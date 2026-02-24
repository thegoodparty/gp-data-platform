{% set source_ref = source(
    "segment_storage_source_web_app", "navigation_dashboard_click_voter_data"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
