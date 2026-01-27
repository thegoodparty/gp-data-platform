{{
    config(
        materialized="incremental",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "position"],
    )
}}

with
    matched_positions as (
        select distinct
            tbl_position.id as id,
            tbl_match.br_database_id,
            tbl_position.br_position_id as br_position_id,
            tbl_match.state,
            tbl_match.l2_district_type,
            tbl_match.confidence,
            tbl_district.id as district_id,
            tbl_position.created_at,
            tbl_position.updated_at
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as tbl_match
        left join
            {{ ref("int__enhanced_position") }} as tbl_position
            on tbl_match.br_database_id = tbl_position.br_database_id
        left join
            {{ ref("m_election_api__district") }} as tbl_district
            on tbl_match.state = tbl_district.state
            and tbl_match.l2_district_type = tbl_district.l2_district_type
            and tbl_match.l2_district_name = tbl_district.l2_district_name
        where
            tbl_match.l2_district_name not in (
                'County Committee Female Member',
                'County Committee Male Member',
                'President of the United States',
                'Vice President of the United States'
            )
            {% if is_incremental() %}
                and tbl_position.updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    )

select id, br_database_id, br_position_id, state, district_id, created_at, updated_at
from matched_positions
where
    district_id is not null
    and (
        (lower(l2_district_type) = 'state' and confidence >= 100)
        or (lower(l2_district_type) != 'state' and confidence >= 95)
    )
