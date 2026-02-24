{% set source_ref = source(
    "segment_storage_source", "content_builder_generation_started"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
