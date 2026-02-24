{% set source_ref = source(
    "segment_storage_source_web_app",
    "voter_data_custom_voter_file_click_next",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
