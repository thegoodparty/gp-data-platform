select *
from {{ source("dbt_source", "l2_s3_wi_vote_history") }}
