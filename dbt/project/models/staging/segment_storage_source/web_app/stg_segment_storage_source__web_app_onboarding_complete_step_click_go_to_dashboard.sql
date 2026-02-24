{% set source_ref = source(
    "segment_storage_source_web_app",
    "onboarding_complete_step_click_go_to_dashboard",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
