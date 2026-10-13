select *
from {{ source("dbt_source", "l2_s3_ct_uniform_data_dictionary") }}
