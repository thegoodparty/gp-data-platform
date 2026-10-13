select * from {{ source("dbt_source", "l2_s3_nh_vote_history") }}
