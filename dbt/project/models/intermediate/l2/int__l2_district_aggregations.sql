{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["state_postal_code", "district_type", "district_name"],
        on_schema_change="fail",
        auto_liquid_cluster=True,
    )
}}

/*
District aggregations: unpivot L2 district columns and count distinct voters
per (state, district_type, district_name). Also emits state-level rows
(district_type='State', district_name=state_postal_code) for statewide
positions (Governor, U.S. Senate) that the L2 matching model maps to these
synthetic district types.

Incremental strategy: identify districts with new data, then re-aggregate ALL
voters for those districts (not just new ones) so voter_count is the total,
not the incremental count.
*/
with
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
    l2_data as (
        select
            state_postal_code,
            lalvoterid,
            votertelephones_cellphoneformatted,
            votertelephones_landlineformatted,
            loaded_at,
            {{ get_l2_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }}
    ),
    l2_data_districts as (
        select
            state_postal_code,
            lalvoterid,
            votertelephones_cellphoneformatted,
            votertelephones_landlineformatted,
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
    filtered_districts as (
        select
            l2_data_districts.state_postal_code,
            l2_data_districts.lalvoterid,
            l2_data_districts.votertelephones_cellphoneformatted,
            l2_data_districts.votertelephones_landlineformatted,
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
    -- Aggregate ALL voters for the districts (including historical voters) so
    -- voter_count is the total, not just the incremental count.
    -- voter_count collapses on lalvoterid (distinct voters); the two phone
    -- counts collapse on the phone-number value itself (so household-shared
    -- phones count once per district, which matches per-channel
    -- send/dial costs downstream).
    district_aggregations as (
        select
            state_postal_code,
            district_type,
            district_name,
            count(distinct lalvoterid) as voter_count,
            count(
                distinct case
                    when
                        votertelephones_cellphoneformatted is not null
                        and trim(votertelephones_cellphoneformatted) != ''
                    then votertelephones_cellphoneformatted
                end
            ) as unique_cellphones,
            count(
                distinct case
                    when
                        votertelephones_landlineformatted is not null
                        and trim(votertelephones_landlineformatted) != ''
                    then votertelephones_landlineformatted
                end
            ) as unique_landlines,
            max(loaded_at) as loaded_at
        from filtered_districts
        group by state_postal_code, district_type, district_name
    ),

    states_with_new_data as (
        {% if is_incremental() %}
            select distinct state_postal_code
            from {{ ref("int__l2_nationwide_uniform") }}
            where
                loaded_at > coalesce(
                    (
                        select max(loaded_at)
                        from {{ this }}
                        where district_type = 'State'
                    ),
                    '1900-01-01'
                )
        {% else %}
            -- For full refresh, this CTE is not used
            select null as state_postal_code where false
        {% endif %}
    ),

    -- State-level rows for statewide positions, matched by the L2 matching
    -- model to district_type='State', district_name=state_postal_code.
    state_aggregations as (
        select
            l2.state_postal_code,
            'State' as district_type,
            l2.state_postal_code as district_name,
            count(distinct l2.lalvoterid) as voter_count,
            count(
                distinct case
                    when
                        l2.votertelephones_cellphoneformatted is not null
                        and trim(l2.votertelephones_cellphoneformatted) != ''
                    then l2.votertelephones_cellphoneformatted
                end
            ) as unique_cellphones,
            count(
                distinct case
                    when
                        l2.votertelephones_landlineformatted is not null
                        and trim(l2.votertelephones_landlineformatted) != ''
                    then l2.votertelephones_landlineformatted
                end
            ) as unique_landlines,
            max(l2.loaded_at) as loaded_at
        from {{ ref("int__l2_nationwide_uniform") }} l2
        {% if is_incremental() %}
            inner join
                states_with_new_data s on l2.state_postal_code = s.state_postal_code
        {% endif %}
        group by l2.state_postal_code
    ),

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
    }} as id,
    state_postal_code,
    district_type,
    district_name,
    voter_count,
    unique_cellphones,
    unique_landlines,
    loaded_at
from all_aggregations
