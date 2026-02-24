{% set source_ref = source("segment_storage_source_gp_api", "tracks") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
