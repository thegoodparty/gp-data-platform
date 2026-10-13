select * from {{ source("dbt_source", "l2_s3_wa_vote_history") }}
