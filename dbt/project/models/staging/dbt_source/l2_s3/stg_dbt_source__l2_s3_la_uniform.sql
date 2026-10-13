select *
from {{ source("dbt_source", "l2_s3_la_uniform") }}
