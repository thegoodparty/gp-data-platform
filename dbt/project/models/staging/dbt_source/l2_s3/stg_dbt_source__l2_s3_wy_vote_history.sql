select *
from {{ source("dbt_source", "l2_s3_wy_vote_history") }}
