select *
from {{ source("dbt_source", "l2_s3_ar_uniform") }}
