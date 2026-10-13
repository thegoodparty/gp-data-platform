select *
from {{ source("dbt_source_haystaq", "l2_s3_ia_haystaq_dna_scores") }}
