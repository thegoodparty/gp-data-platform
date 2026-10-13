select *
from {{ source("dbt_source", "l2_s3_mn_uniform_data_dictionary") }}
