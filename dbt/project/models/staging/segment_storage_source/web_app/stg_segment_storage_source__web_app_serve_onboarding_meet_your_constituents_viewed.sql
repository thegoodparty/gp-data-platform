{% set source_ref = source(
    "segment_storage_source_web_app",
    "serve_onboarding_meet_your_constituents_viewed",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
