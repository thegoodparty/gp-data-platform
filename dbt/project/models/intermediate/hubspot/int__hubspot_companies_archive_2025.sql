-- Archived HubSpot companies prioritizing 2025 election dates
-- Selects from snapshot to get historical data with 2025 election dates
{% set snapshot_columns = adapter.get_columns_in_relation(
    ref("snapshot__hubspot_api_companies")
) %}
{% set snapshot_column_names = (
    snapshot_columns | map(attribute="name") | map("lower") | list
) %}
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
    * except (dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to),
    -- Recovered from the properties JSON. The HubSpot companies stream no longer
    -- narrows this property into its own raw column, so a snapshot taken after
    -- that change has no properties_win_number and this model -- a select * -- had
    -- no column to pass downstream. The value still arrives inside properties,
    -- so restore it under the name the models built on this one expect. Guarded
    -- on the snapshot's own columns so a snapshot that still carries the column
    -- keeps it, and cast to that column's decimal type so this is a restore and
    -- not a retype.
    {% if "properties_win_number" not in snapshot_column_names %}
        try_cast(
            nullif(get_json_object(properties, '$.win_number'), '')
            as decimal(38, 10)
        ) as properties_win_number
    {% endif %}
    -- fmt: on
from current_version
where {{ dsar_not_suppressed("properties_candidate_email", "email") }}
