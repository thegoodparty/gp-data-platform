{% set source_ref = source(
    "segment_storage_source_web_app", "onboarding_click_finish_later"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
