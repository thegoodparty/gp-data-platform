-- Schema evolution lets this view follow columns L2 drops without a rebuild.
{{ config(post_hook="alter view {{ this }} with schema evolution") }}

select *
from {{ source("dbt_source", "l2_s3_la_vote_history") }}
