select *
from {{ source("dbt_source", "l2_s3_hi_uniform_data_dictionary") }}
