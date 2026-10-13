select *
from {{ source("dbt_source", "l2_s3_nd_vote_history") }}
