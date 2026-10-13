select *
from {{ source("dbt_source", "l2_s3_co_vote_history") }}
