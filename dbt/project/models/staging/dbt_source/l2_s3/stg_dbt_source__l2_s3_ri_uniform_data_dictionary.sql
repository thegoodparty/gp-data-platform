select *
from {{ source("dbt_source", "l2_s3_ri_uniform_data_dictionary") }}
