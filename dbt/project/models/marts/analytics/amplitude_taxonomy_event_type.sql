{{ config(materialized="view") }}

select *
from {{ ref("stg_airbyte_source__amplitude_taxonomy_event_type") }}
