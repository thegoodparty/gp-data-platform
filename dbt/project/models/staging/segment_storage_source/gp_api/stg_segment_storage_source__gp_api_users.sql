{% set source_ref = source("segment_storage_source", "users") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
