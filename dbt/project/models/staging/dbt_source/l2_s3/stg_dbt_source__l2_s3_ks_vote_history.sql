select * from {{ source("dbt_source", "l2_s3_ks_vote_history") }}
