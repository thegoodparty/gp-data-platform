select *
from {{ source("dbt_source", "l2_s3_ny_uniform_data_dictionary") }}
