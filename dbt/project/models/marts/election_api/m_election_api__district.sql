{{
    config(
        materialized="incremental",
        unique_key="id",
        on_schema_change="fail",
        auto_liquid_cluster=True,
    )
}}

with
    turnout_districts as (
        select distinct
            state, district_type as l2_district_type, district_name as l2_district_name
        from {{ ref("int__model_prediction_voter_turnout") }}
        {% if is_incremental() %}
            where inference_at >= (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    l2_data as (
        select
            state_postal_code,
            {{ get_l2_district_columns(use_backticks=true, cast_to_string=true) }}
        from {{ ref("int__l2_nationwide_uniform") }}
        {% if is_incremental() %}
            where loaded_at >= (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    -- States whose L2 proposed congressional map is the one legally in force
    -- for the upcoming election, and has been checked. A proposed value is not
    -- evidence of adoption (see the district_map_adoption seed), so this is the
    -- only route by which a proposed district becomes bindable.
    -- district_number is a sparse key: null means the decision covers every
    -- district in the state, a value narrows it to that one. Adoption is a
    -- state-wide legal fact, so state-level rows are the norm; per-district
    -- rows exist to roll out districts as each is verified against the
    -- enacted map, without holding the whole state back.
    adopted_proposed_congressional as (
        select state, cast(nullif(trim(district_number), '') as int) as district_number
        from {{ ref("district_map_adoption") }}
        where
            district_type = 'US_Congressional_District'
            and adopted_source = 'proposed'
            and is_verified
    ),
    l2_data_districts_raw as (
        select distinct
            state_postal_code as state,
            district_column_name as l2_district_type,
            district_value as l2_district_name
        from
            l2_data unpivot (
                district_value for district_column_name
                in ({{ get_l2_district_columns(use_backticks=false) }})
            )
        where district_value is not null
    ),
    -- Proposed_District is heterogeneous: proposed congressional maps, MI's
    -- proposed state senate, and local annexations. Only adopted congressional
    -- values pass; everything else in the column is dropped rather than minted
    -- as an inert row nothing should reach.
    l2_data_districts as (
        select raw.state, raw.l2_district_type, raw.l2_district_name
        from l2_data_districts_raw as raw
        where
            raw.l2_district_type != 'Proposed_District'
            or exists (
                select 1
                from adopted_proposed_congressional as adoption
                where
                    adoption.state = raw.state
                    and {{ is_proposed_cong_dist("raw.l2_district_name") }}
                    and (
                        adoption.district_number is null
                        or adoption.district_number
                        = {{ proposed_cong_dist_number("raw.l2_district_name") }}
                    )
            )
    ),
    -- State-level districts for statewide positions (Governor, US Senate, etc.)
    state_districts as (
        select distinct
            state_postal_code as state,
            'State' as l2_district_type,
            state_postal_code as l2_district_name
        from {{ ref("int__l2_nationwide_uniform") }}
        {% if is_incremental() %}
            where loaded_at >= (select max(updated_at) from {{ this }})
        {% endif %}
    ),
    unioned_w_id_districts as (
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "turnout_districts.state",
                        "turnout_districts.l2_district_type",
                        "turnout_districts.l2_district_name",
                    ]
                )
            }} as id, turnout_districts.*
        from turnout_districts
        union all
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "l2_data_districts.state",
                        "l2_data_districts.l2_district_type",
                        "l2_data_districts.l2_district_name",
                    ]
                )
            }} as id, l2_data_districts.*
        from l2_data_districts
        union all
        select
            {{
                generate_salted_uuid(
                    fields=[
                        "state_districts.state",
                        "state_districts.l2_district_type",
                        "state_districts.l2_district_name",
                    ]
                )
            }} as id, state_districts.*
        from state_districts
    ),
    districts as (select * from unioned_w_id_districts)

select
    districts.id,
    {% if is_incremental() %} coalesce(existing.created_at, now()) as created_at,
    {% else %} now() as created_at,
    {% endif %}
    current_timestamp() as updated_at,
    districts.state,
    districts.l2_district_type,
    districts.l2_district_name,
    -- L2-derived voter aggregates. Joined on the salted-id match
    -- (m_election_api__district and int__l2_district_aggregations
    -- both hash the same (state, l2_district_type, l2_district_name)
    -- tuple with the default salt). Turnout-only synthetic districts
    -- have no L2 row and surface NULL across all three.
    tbl_agg.voter_count as registered_voters,
    tbl_agg.unique_cellphones,
    tbl_agg.unique_landlines
from districts
left join
    {{ ref("int__l2_district_aggregations") }} as tbl_agg on districts.id = tbl_agg.id
{% if is_incremental() %}
    left join
        {{ this }} as existing
        on {{
            generate_salted_uuid(
                fields=[
                    "districts.state",
                    "districts.l2_district_type",
                    "districts.l2_district_name",
                ]
            )
        }} = existing.id
{% endif %}
qualify row_number() over (partition by districts.id order by updated_at desc) = 1
