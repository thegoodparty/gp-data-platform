select *
from {{ source("dbt_source", "l2_s3_az_uniform_data_dictionary") }}
