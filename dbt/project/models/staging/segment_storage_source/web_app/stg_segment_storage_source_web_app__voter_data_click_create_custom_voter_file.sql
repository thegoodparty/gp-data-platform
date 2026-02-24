{% set source_ref = source(
    "segment_storage_source_web_app",
    "voter_data_click_create_custom_voter_file",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
