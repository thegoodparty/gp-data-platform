select *
from {{ source("dbt_source_haystaq", "l2_s3_nv_haystaq_dna_flags") }}
