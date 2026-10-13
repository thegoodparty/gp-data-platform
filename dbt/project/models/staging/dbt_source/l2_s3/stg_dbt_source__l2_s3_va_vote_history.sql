select *
from {{ source("dbt_source", "l2_s3_va_vote_history") }}
