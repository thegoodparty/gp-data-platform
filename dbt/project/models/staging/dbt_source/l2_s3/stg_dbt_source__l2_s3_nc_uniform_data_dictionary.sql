select *
from {{ source("dbt_source", "l2_s3_nc_uniform_data_dictionary") }}
