select *
from {{ source("dbt_source", "l2_s3_ia_uniform_data_dictionary") }}
