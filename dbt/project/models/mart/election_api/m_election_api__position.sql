{{
    config(
        materialized="table",
        unique_key="id",
        auto_liquid_cluster=true,
        tags=["mart", "election_api", "position"],
    )
}}

with
    positions as (
        select distinct
            tbl_position.id as id,
            tbl_match.br_database_id,
            tbl_position.br_position_id as br_position_id,
            tbl_match.state,
            tbl_district.id as district_id
        from {{ ref("stg_model_predictions__llm_l2_br_match_20250806") }} as tbl_match
        left join
            {{ ref("int__enhanced_position") }} as tbl_position
            on tbl_match.br_database_id = tbl_position.br_database_id
        left join
            {{ ref("m_election_api__district") }} as tbl_district
            on tbl_match.state = tbl_district.state
            and tbl_match.l2_district_type = tbl_district.l2_district_type
            and tbl_match.l2_district_name = tbl_district.l2_district_name
        {% if is_incremental() %}
            where tbl_position.updated_at > (select max(updated_at) from {{ this }})
        {% endif %}
    )

select id, br_database_id, br_position_id, state, district_id
from positions
