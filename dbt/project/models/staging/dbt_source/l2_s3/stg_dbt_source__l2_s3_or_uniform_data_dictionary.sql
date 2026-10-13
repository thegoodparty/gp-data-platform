select *
from {{ source("dbt_source", "l2_s3_or_uniform_data_dictionary") }}
