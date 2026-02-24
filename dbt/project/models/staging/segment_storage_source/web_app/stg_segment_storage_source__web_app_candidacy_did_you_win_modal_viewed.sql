{% set source_ref = source(
    "segment_storage_source_web_app", "candidacy_did_you_win_modal_viewed"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
