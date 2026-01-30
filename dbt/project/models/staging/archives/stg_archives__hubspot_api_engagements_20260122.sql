-- Archived HubSpot engagements from 2026-01-22
-- No snapshot exists for engagements, so source directly from archive table
select *
from {{ source("archives", "airbyte_source__hubspot_api_engagements_20260122") }}
