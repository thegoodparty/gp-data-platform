{% set source_ref = source(
    "segment_storage_source_web_app", "voter_data_file_detail_click_back"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
