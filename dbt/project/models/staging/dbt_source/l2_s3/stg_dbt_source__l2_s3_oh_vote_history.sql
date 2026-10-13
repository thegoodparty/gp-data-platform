select * from {{ source("dbt_source", "l2_s3_oh_vote_history") }}
