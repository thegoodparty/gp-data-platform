{% set source_ref = source(
    "segment_storage_source_web_app",
    "profile_top_issues_click_finish_entering_issues",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
