{% set source_ref = source(
    "segment_storage_source_web_app",
    "dashboard_path_to_victory_exit_understand_path_to_victory",
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
