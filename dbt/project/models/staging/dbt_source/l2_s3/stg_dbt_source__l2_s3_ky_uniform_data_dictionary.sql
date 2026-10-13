select *
from {{ source("dbt_source", "l2_s3_ky_uniform_data_dictionary") }}
