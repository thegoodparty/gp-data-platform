select *
from {{ source("dbt_source", "l2_s3_oh_uniform_data_dictionary") }}
