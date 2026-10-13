select *
from {{ source("dbt_source", "l2_s3_tn_vote_history") }}
