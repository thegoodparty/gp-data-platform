{{ config(materialized="view") }}

-- Exposure of stg_airbyte_source__amplitude_taxonomy_event_type (the Amplitude
-- Govern declared event universe, ~434 events) into mart_analytics so governance
-- consumers read the mart. Thin 1:1 view; no storage duplication.
select *
from {{ ref("stg_airbyte_source__amplitude_taxonomy_event_type") }}
