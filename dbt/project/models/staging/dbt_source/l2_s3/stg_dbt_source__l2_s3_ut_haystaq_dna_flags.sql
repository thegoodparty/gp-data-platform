select *
from {{ source("dbt_source_haystaq", "l2_s3_ut_haystaq_dna_flags") }}
