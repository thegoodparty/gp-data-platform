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

Additionally, it creates state-level aggregations (district_type='State',
district_name=state_postal_code) for statewide positions like Governor and
U.S. Senate, which are matched to these synthetic district types by the
L2 matching model.

Output schema:
    - state_postal_code: String (two-letter state code)
    - district_type: String (e.g., "US_Congressional_District", "State_Senate_District", "State")
    - district_name: String (the actual district identifier/name, or state code for statewide)
    - voter_count: Long (number of voters in this district)
    - loaded_at: Timestamp (from the source data)

Performance notes (as of 2026-01-14):
    - Full refresh: ~15 minutes
    - Incremental run: ~2 minutes

Incremental strategy:
    For incremental runs, we identify districts that have new data, then re-aggregate
    ALL voters for those districts (not just new ones). This ensures voter_count
    represents the total count, not just the incremental count.
*/
with
    -- Step 1: Identify districts that have new data (for incremental runs only)
    districts_with_new_data as (
        {% if is_incremental() %}
            select distinct
                state_postal_code,
                district_column_name as district_type,
                district_value as district_name
            from
                (
                    select
                        state_postal_code,
                        lalvoterid,
                        loaded_at,
                        {{
                            get_l2_district_columns(
                                use_backticks=true, cast_to_string=true
                            )
                        }}
                    from {{ ref("int__l2_nationwide_uniform") }}
                    where
                        loaded_at > coalesce(
                            (select max(loaded_at) from {{ this }}), '1900-01-01'
                        )
                ) unpivot (
                    district_value for district_column_name
                    in ({{ get_l2_district_columns(use_backticks=false) }})
                )
            where district_value is not null
        {% else %}
            -- For full refresh, this CTE is not used
            select
                null as state_postal_code, null as district_type, null as district_name
            where false
        {% endif %}
    ),
    -- Step 2: Get all L2 data (full table for incremental, filtered only for
    -- performance)
    l2_data as (
        select
            state_postal_code,
            lalvoterid,
            loaded_at,
            {{ get_l2_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }}
    ),
    -- Step 3: Unpivot all district columns
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
    -- Step 4: Filter to districts with new data (for incremental) or all districts
    -- (for full refresh)
    filtered_districts as (
        select
            l2_data_districts.state_postal_code,
            l2_data_districts.lalvoterid,
            l2_data_districts.district_type,
            l2_data_districts.district_name,
            l2_data_districts.loaded_at
        from l2_data_districts
        {% if is_incremental() %}
            inner join
                districts_with_new_data
                on l2_data_districts.state_postal_code
                = districts_with_new_data.state_postal_code
                and l2_data_districts.district_type
                = districts_with_new_data.district_type
                and l2_data_districts.district_name
                = districts_with_new_data.district_name
        {% endif %}
    ),
    -- Step 5: Aggregate all voters for the districts (including historical voters)
    -- This ensures voter_count represents the total, not just incremental count
    district_aggregations as (
        select
            state_postal_code,
            district_type,
            district_name,
            count(distinct lalvoterid) as voter_count,
            max(loaded_at) as loaded_at
        from filtered_districts
        group by state_postal_code, district_type, district_name
    ),

    -- Step 6: State-level aggregations for statewide positions (Governor, US Senate,
    -- etc.)
    -- These positions are matched to l2_district_type='State' and
    -- l2_district_name=state_postal_code
    state_aggregations as (
        select
            state_postal_code,
            'State' as district_type,
            state_postal_code as district_name,
            count(distinct lalvoterid) as voter_count,
            max(loaded_at) as loaded_at
        from {{ ref("int__l2_nationwide_uniform") }}
        {% if is_incremental() %}
            where
                loaded_at > coalesce(
                    (
                        select max(loaded_at)
                        from {{ this }}
                        where district_type = 'State'
                    ),
                    '1900-01-01'
                )
        {% endif %}
        group by state_postal_code
    ),

    -- Step 7: Combine district and state aggregations
    all_aggregations as (
        select *
        from district_aggregations
        union all
        select *
        from state_aggregations
    )

select
    {{
        generate_salted_uuid(
            fields=[
                "all_aggregations.state_postal_code",
                "all_aggregations.district_type",
                "all_aggregations.district_name",
            ]
        )
    }} as id, state_postal_code, district_type, district_name, voter_count, loaded_at
from all_aggregations
