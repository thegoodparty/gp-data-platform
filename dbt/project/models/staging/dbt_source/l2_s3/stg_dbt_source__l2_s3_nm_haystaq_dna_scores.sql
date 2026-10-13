select *
from {{ source("dbt_source_haystaq", "l2_s3_nm_haystaq_dna_scores") }}
