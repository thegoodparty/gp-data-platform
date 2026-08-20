with
    turnout_districts as (
        select distinct
            state, district_type as l2_district_type, district_name as l2_district_name
        from {{ ref("int__model_prediction_voter_turnout") }}
    ),
    -- Carries the synthetic district_type='State' rows statewide positions match on.
    l2_districts as (
        select
            state_postal_code as state,
            district_type as l2_district_type,
            district_name as l2_district_name
        from {{ ref("int__l2_district_aggregations") }}
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
                        "l2_districts.state",
                        "l2_districts.l2_district_type",
                        "l2_districts.l2_district_name",
                    ]
                )
            }} as id, l2_districts.*
        from l2_districts
    ),
    districts as (select * from unioned_w_id_districts)

select
    districts.id,
    now() as created_at,
    current_timestamp() as updated_at,
    districts.state,
    districts.l2_district_type,
    districts.l2_district_name,
    -- L2-derived voter aggregates. Joined on the salted-id match
    -- (m_election_api__district and int__l2_district_aggregations
    -- both hash the same (state, l2_district_type, l2_district_name)
    -- tuple with the default salt). Turnout-only synthetic districts
    -- have no L2 row and surface NULL across all three.
    agg.voter_count as registered_voters,
    agg.unique_cellphones,
    agg.unique_landlines
from districts
left join {{ ref("int__l2_district_aggregations") }} as agg on districts.id = agg.id
qualify row_number() over (partition by districts.id order by updated_at desc) = 1
