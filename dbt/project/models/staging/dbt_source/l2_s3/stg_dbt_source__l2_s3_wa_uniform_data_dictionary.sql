select *
from {{ source("dbt_source", "l2_s3_wa_uniform_data_dictionary") }}
