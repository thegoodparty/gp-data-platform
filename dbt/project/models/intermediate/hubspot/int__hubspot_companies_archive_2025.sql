-- Archived HubSpot companies prioritizing 2025 election dates
-- Selects from snapshot to get historical data with 2025 election dates
with
    -- Pick the version first, then suppress, so a registered value on one version
    -- removes the company rather than promoting an older version of it.
    current_version as (
        select *
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
                    dbt_valid_from desc
            )
            = 1
    )

select
    -- fmt: off
    * except (dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to)
    -- fmt: on
from current_version
where
    {{ dsar_not_suppressed("properties_candidate_email", "email") }}
    and {{ dsar_not_suppressed("properties_phone", "phone") }}
