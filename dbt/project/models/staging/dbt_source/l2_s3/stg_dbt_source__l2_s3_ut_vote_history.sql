select *
from {{ source("dbt_source", "l2_s3_ut_vote_history") }}
