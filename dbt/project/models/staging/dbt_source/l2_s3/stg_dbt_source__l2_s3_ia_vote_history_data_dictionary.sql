{% set source_ref = source("dbt_source", "l2_s3_ia_vote_history_data_dictionary") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}  -- use `except` for any columns to transform individually
from {{ source_ref }}
