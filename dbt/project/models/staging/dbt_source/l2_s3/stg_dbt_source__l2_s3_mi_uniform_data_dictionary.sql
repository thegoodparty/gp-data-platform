select *
from {{ source("dbt_source", "l2_s3_mi_uniform_data_dictionary") }}
