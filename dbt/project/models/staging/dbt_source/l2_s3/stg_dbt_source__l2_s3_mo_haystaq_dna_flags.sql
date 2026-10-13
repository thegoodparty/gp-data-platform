select *
from {{ source("dbt_source_haystaq", "l2_s3_mo_haystaq_dna_flags") }}
