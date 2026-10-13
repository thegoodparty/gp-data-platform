select *
from {{ source("dbt_source", "l2_s3_al_uniform_data_dictionary") }}
