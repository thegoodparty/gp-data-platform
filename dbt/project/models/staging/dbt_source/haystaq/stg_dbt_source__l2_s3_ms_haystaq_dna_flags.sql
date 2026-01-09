{{ config(materialized="view") }}

{% set source_ref = source("dbt_source_haystaq", "l2_s3_ms_haystaq_dna_flags") %}

select {{ dbt_utils.star(from=source_ref, except=[]) }}
from {{ source_ref }}
