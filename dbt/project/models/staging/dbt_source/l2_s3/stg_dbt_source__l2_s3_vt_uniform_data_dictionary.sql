select *
from {{ source("dbt_source", "l2_s3_vt_uniform_data_dictionary") }}
