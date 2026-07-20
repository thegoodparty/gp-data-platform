{{ config(materialized="view") }}

select *
from {{ ref("stg_airbyte_source__amplitude_api_events") }}
