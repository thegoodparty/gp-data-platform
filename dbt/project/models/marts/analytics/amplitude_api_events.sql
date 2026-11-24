{{ config(materialized="view") }}

select *
from {{ ref("stg_amplitude_ingestion__events") }}
