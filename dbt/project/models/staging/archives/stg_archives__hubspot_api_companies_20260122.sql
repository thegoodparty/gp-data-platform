-- Archived HubSpot companies prioritizing 2025 election dates
-- Sources from archive table, inner joined with snapshot to apply election date
-- prioritization
with
    -- Get prioritized ids from snapshot based on 2025 election dates
    snapshot_prioritized as (
        select id
        from {{ ref("snapshot__hubspot_api_companies") }}
        qualify
            row_number() over (
                partition by id
                order by
                    case
                        when year(properties_election_date) = 2025
                        then properties_election_date
                    end desc nulls last,
                    case
                        when year(properties_primary_date) = 2025
                        then properties_primary_date
                    end desc nulls last,
                    case
                        when year(properties_runoff_date) = 2025
                        then properties_runoff_date
                    end desc nulls last,
                    updatedat desc
            )
            = 1
    )

select archive.*
from
    {{ source("archives", "airbyte_source__hubspot_api_companies_20260122") }}
    as archive
inner join snapshot_prioritized on archive.id = snapshot_prioritized.id
