{% set source_ref = source(
    "segment_storage_source_web_app", "polls_create_poll_clicked"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
