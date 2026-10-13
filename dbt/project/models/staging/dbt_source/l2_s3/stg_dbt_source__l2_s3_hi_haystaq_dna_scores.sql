select *
from {{ source("dbt_source_haystaq", "l2_s3_hi_haystaq_dna_scores") }}
