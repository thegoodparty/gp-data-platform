{{ config(materialized="view") }}

-- Exposure of int__amplitude_event_catalog into mart_analytics so governance
-- consumers read the mart (grants live at the mart schema, not per relation).
-- Thin view over an already-materialized table; no storage duplication.
select *
from {{ ref("int__amplitude_event_catalog") }}
