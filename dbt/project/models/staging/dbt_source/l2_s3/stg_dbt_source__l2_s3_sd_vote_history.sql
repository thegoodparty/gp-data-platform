select *
from {{ source("dbt_source", "l2_s3_sd_vote_history") }}
