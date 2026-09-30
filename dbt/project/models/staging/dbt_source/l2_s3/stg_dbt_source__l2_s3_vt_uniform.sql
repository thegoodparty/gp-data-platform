{% set source_ref = source("dbt_source", "l2_s3_vt_uniform") %}

select {{ l2_uniform_columns(source_ref) }}
from {{ source_ref }}
