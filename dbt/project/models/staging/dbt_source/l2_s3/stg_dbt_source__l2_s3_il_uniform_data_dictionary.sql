select *
from {{ source("dbt_source", "l2_s3_il_uniform_data_dictionary") }}
