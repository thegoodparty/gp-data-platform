select *
from {{ source("dbt_source", "l2_s3_mn_vote_history") }}
