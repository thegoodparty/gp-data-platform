select *
from {{ source("dbt_source_haystaq", "l2_s3_al_haystaq_dna_flags") }}
