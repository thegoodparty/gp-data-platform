select *
from {{ source("dbt_source_haystaq", "l2_s3_ny_haystaq_dna_scores") }}
