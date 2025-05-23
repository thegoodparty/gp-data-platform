{% set source_ref = source("dbt_source", "l2_s3_pa_vote_history") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}  -- use `except` for any columns to transform individually
from {{ source_ref }}
