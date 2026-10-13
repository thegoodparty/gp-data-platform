select *
from {{ source("dbt_source", "l2_s3_fl_uniform_data_dictionary") }}
