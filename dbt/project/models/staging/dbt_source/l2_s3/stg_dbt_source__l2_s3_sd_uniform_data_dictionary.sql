select *
from {{ source("dbt_source", "l2_s3_sd_uniform_data_dictionary") }}
