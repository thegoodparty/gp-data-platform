select *
from {{ source("dbt_source", "l2_s3_ia_uniform") }}
