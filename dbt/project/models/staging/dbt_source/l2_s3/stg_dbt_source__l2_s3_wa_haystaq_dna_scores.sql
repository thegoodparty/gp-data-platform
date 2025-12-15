{{ config(materialized="view") }}

{% set source_ref = source("dbt_source_haystaq", "l2_s3_wa_haystaq_dna_scores") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
