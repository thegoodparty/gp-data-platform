select *
from {{ source("dbt_source", "l2_s3_wi_uniform_data_dictionary") }}
