{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["state_postal_code", "district_type", "district_name"],
        on_schema_change="fail",
        auto_liquid_cluster=True,
        tags=["intermediate", "l2", "district_aggregations", "voter_counts"],
    )
}}

/*
This model creates district aggregations by counting voters per district.
It unpivots the district columns from the L2 nationwide uniform data and
aggregates voter counts by state, district type, and district name.

Output schema:
    - state_postal_code: String (two-letter state code)
    - district_type: String (e.g., "US_Congressional_District", "State_Senate_District")
    - district_name: String (the actual district identifier/name)
    - voter_count: Long (number of voters in this district)
    - loaded_at: Timestamp (from the source data)

Performance notes (as of 2026-01-14):
    - Full refresh: ~15 minutes
    - Incremental run: ~30 seconds
*/
with
    l2_data as (
        select
            state_postal_code,
            lalvoterid,
            loaded_at,
            {{ get_l2_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }}
        {% if is_incremental() %}
            where loaded_at > (select max(loaded_at) from {{ this }})
        {% endif %}
    ),
    l2_data_districts as (
        select
            state_postal_code,
            lalvoterid,
            district_column_name as district_type,
            district_value as district_name,
            loaded_at
        from
            l2_data unpivot (
                district_value for district_column_name
                in ({{ get_l2_district_columns(use_backticks=false) }})
            )
        where district_value is not null
    ),
    district_aggregations as (
        select
            state_postal_code,
            district_type,
            district_name,
            count(distinct lalvoterid) as voter_count,
            max(loaded_at) as loaded_at
        from l2_data_districts
        group by state_postal_code, district_type, district_name
    )

select
    {{
        generate_salted_uuid(
            fields=[
                "district_aggregations.state_postal_code",
                "district_aggregations.district_type",
                "district_aggregations.district_name",
            ]
        )
    }} as id, state_postal_code, district_type, district_name, voter_count, loaded_at
from district_aggregations
