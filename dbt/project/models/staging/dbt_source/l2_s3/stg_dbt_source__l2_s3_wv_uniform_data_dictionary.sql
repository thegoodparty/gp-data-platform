select *
from {{ source("dbt_source", "l2_s3_wv_uniform_data_dictionary") }}
