{% set source_ref = source(
    "segment_storage_source_gp_api", "onboarding_user_created"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
