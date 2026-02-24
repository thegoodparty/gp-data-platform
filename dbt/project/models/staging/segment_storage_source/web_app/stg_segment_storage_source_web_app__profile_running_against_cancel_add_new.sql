{% set source_ref = source(
    "segment_storage_source_web_app", "profile_running_against_cancel_add_new"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
