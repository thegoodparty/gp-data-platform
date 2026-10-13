select *
from {{ source("dbt_source_haystaq", "l2_s3_ga_haystaq_dna_scores") }}
