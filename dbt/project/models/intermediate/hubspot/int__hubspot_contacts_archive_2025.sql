-- Archived HubSpot contacts prioritizing 2025 election dates
-- Selects from snapshot to get historical data with 2025 election dates
select
    -- fmt: off
    * except (dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to, createdAt, updatedAt),
    createdAt as created_at,
    updatedAt as updated_at
    -- fmt: on
from {{ ref("snapshot__hubspot_api_contacts") }}
qualify
    row_number() over (
        partition by id
        order by
            case
                when year(properties_general_election_date) = 2025
                then properties_general_election_date
            end desc nulls last,
            case
                when year(properties_election_date) = 2025 then properties_election_date
            end desc nulls last,
            case
                when year(properties_primary_election_date) = 2025
                then properties_primary_election_date
            end desc nulls last,
            updatedat desc
    )
    = 1
