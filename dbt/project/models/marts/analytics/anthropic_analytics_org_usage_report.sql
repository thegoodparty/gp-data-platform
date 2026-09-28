{{ config(materialized="view") }}

-- Sigma reads marts only; thin view over
-- stg_airbyte_source__anthropic_api_usage_report, no storage worth duplicating.
select *
from {{ ref("stg_airbyte_source__anthropic_api_usage_report") }}
