select * from {{ source("dbt_source", "l2_s3_ky_vote_history") }}
