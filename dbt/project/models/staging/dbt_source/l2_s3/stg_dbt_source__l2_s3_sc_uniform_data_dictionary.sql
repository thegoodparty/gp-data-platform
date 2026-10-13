select *
from {{ source("dbt_source", "l2_s3_sc_uniform_data_dictionary") }}
