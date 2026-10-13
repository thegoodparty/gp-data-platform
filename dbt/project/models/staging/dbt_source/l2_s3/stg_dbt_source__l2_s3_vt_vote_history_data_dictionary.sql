select *
from {{ source("dbt_source", "l2_s3_vt_vote_history_data_dictionary") }}
