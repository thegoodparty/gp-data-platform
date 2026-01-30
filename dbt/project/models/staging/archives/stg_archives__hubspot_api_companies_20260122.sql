-- Archived HubSpot companies from 2026-01-22
-- Sources directly from archive table
select * from {{ source("archives", "airbyte_source__hubspot_api_companies_20260122") }}
