select *
from {{ source("dbt_source", "l2_s3_ri_vote_history") }}
