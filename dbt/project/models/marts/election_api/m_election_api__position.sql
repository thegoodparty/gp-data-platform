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
            tbl_position.name,
            coalesce(tbl_override.state, tbl_match.state) as state,
            tbl_district.id as district_id,
            tbl_position.created_at,
            tbl_position.updated_at
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as tbl_match
        inner join
            {{ ref("int__enhanced_position") }} as tbl_position
            on tbl_match.br_database_id = tbl_position.br_database_id
        -- Manual overrides for incorrect LLM matches. The LLM can mismap
        -- at-large positions to numbered sub-districts (e.g. KC Council
        -- At-Large District 3 → City_Council_Commissioner_District instead
        -- of City). At-large seats are voted on by the entire city/state, so
        -- they must map to the city-wide or state-wide L2 district, not the
        -- numbered sub-district they're associated with.
        left join
            {{ ref("l2_br_match_overrides") }} as tbl_override
            on tbl_match.br_database_id = tbl_override.br_database_id
        left join
            {{ ref("m_election_api__district") }} as tbl_district
            on coalesce(tbl_override.state, tbl_match.state) = tbl_district.state
            and coalesce(tbl_override.l2_district_type, tbl_match.l2_district_type)
            = tbl_district.l2_district_type
            and coalesce(tbl_override.l2_district_name, tbl_match.l2_district_name)
            = tbl_district.l2_district_name
        where
            tbl_match.l2_district_name not in (
                'County Committee Female Member',
                'County Committee Male Member',
                'President of the United States',
                'Vice President of the United States'
            )
            and tbl_district.id is not null
            and (
                tbl_override.br_database_id is not null
                or (
                    lower(tbl_match.l2_district_type) = 'state'
                    and tbl_match.confidence >= 95
                )
                or (
                    lower(tbl_match.l2_district_type) != 'state'
                    and tbl_match.confidence >= 90
                )
            )
            {% if is_incremental() %}
                and tbl_position.updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    ),

    unmatched_br_positions as (
        select
            tbl_position.id as id,
            tbl_position.br_database_id,
            tbl_position.br_position_id as br_position_id,
            tbl_position.name,
            tbl_position.state,
            cast(null as string) as district_id,
            tbl_position.created_at,
            tbl_position.updated_at
        from {{ ref("int__enhanced_position") }} as tbl_position
        where
            tbl_position.br_database_id not in (
                select br_database_id
                from matched_positions
                where br_database_id is not null
            )
            {% if is_incremental() %}
                and tbl_position.updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    )

select
    id, br_database_id, br_position_id, name, state, district_id, created_at, updated_at
from matched_positions
union all
select
    id, br_database_id, br_position_id, name, state, district_id, created_at, updated_at
from unmatched_br_positions
