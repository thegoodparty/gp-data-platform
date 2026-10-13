select *
from {{ source("dbt_source", "l2_s3_nv_vote_history_data_dictionary") }}
