{% set source_ref = source(
    "segment_storage_source_web_app", "p2p_upgrade_modal_modal_shown"
) %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
