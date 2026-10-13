select *
from {{ source("dbt_source", "l2_s3_me_vote_history") }}
