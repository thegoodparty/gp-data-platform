select * from {{ source("dbt_source", "l2_s3_sc_vote_history") }}
