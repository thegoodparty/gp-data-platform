{% set source_ref = source(
    "segment_storage_source_web_app", "onboarding_party_step_click_submit"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
