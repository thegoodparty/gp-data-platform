select *
from {{ source("dbt_source", "l2_s3_ms_vote_history") }}
