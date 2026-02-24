{% set source_ref = source(
    "segment_storage_source_web_app", "outreach_action_clicked"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
