select *
from {{ source("dbt_source", "l2_s3_ms_uniform_data_dictionary") }}
