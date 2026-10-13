select *
from {{ source("dbt_source", "l2_s3_ok_vote_history") }}
