select *
from {{ source("dbt_source", "l2_s3_me_uniform_data_dictionary") }}
