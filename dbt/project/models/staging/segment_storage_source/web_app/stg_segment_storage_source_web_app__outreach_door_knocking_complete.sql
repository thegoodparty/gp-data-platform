{% set source_ref = source(
    "segment_storage_source_web_app", "outreach_door_knocking_complete"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
