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

    {%- set recovered_win_number -%}
        try_cast(
            nullif(get_json_object(properties, '$.win_number'), '')
            as decimal(38, 10)
        )
    {%- endset -%}

select
    -- fmt: off
    * except (
        dbt_scd_id, dbt_updated_at, dbt_valid_from, dbt_valid_to
        {%- if "properties_win_number" in snapshot_column_names %}, properties_win_number{% endif %}
    ),
    -- Recovered from the properties JSON, and recovered on both shapes of
    -- snapshot. The HubSpot companies stream stopped narrowing this property
    -- into its own raw column, so a snapshot taken after that change has no
    -- properties_win_number; one taken before still has the column, but its
    -- later versions leave it NULL while the JSON carries the value. A dbt
    -- snapshot adds columns and never drops them, so prod is the second shape:
    -- the column exists and is nearly empty (497 populated of 70,640 rows, last
    -- written 2026-03-19). Recovering only when the column is absent repaired
    -- fresh schemas and left prod broken, which is backwards. Coalesce keeps
    -- whichever value each version actually has; the cast matches the column's
    -- own decimal type so this is a restore, not a retype.
    coalesce(
        {%- if "properties_win_number" in snapshot_column_names %}
        properties_win_number,
        {% endif %}
        {{ recovered_win_number }}
    ) as properties_win_number
    -- fmt: on
from current_version
where {{ dsar_not_suppressed("properties_candidate_email", "email") }}
