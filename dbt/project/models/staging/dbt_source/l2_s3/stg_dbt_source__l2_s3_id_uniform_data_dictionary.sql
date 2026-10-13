select *
from {{ source("dbt_source", "l2_s3_id_uniform_data_dictionary") }}
