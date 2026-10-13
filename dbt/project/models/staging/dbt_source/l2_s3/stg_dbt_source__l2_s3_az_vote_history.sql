select *
from {{ source("dbt_source", "l2_s3_az_vote_history") }}
