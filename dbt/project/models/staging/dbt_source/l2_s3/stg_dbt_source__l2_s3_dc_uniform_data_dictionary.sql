select *
from {{ source("dbt_source", "l2_s3_dc_uniform_data_dictionary") }}
