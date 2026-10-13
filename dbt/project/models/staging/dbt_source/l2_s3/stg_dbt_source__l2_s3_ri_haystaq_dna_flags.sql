select *
from {{ source("dbt_source_haystaq", "l2_s3_ri_haystaq_dna_flags") }}
