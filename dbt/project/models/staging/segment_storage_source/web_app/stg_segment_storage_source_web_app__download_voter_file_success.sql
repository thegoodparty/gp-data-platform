{% set source_ref = source(
    "segment_storage_source_web_app", "download_voter_file_success"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
