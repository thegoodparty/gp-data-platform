select *
from {{ source("dbt_source", "l2_s3_ga_uniform") }}
