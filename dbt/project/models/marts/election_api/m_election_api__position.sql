{{
    config(
        materialized="incremental",
        unique_key="id",
        on_schema_change="append_new_columns",
        auto_liquid_cluster=true,
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
            tbl_position.level,
            tbl_district.id as district_id,
            tbl_position.created_at,
            tbl_position.updated_at
        from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }} as tbl_match
        inner join
            {{ ref("int__enhanced_position") }} as tbl_position
            on tbl_match.br_database_id = tbl_position.br_database_id
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
                and (
                    tbl_position.updated_at > (select max(updated_at) from {{ this }})
                    or tbl_override.br_database_id is not null
                )
            {% endif %}
    ),

    -- Inject a match from the override seed for positions absent from the LLM
    -- snapshot (the left join above can only correct rows that exist there).
    override_injected_positions as (
        select distinct
            tbl_position.id as id,
            tbl_override.br_database_id,
            tbl_position.br_position_id as br_position_id,
            tbl_position.name,
            tbl_override.state,
            tbl_position.level,
            tbl_district.id as district_id,
            tbl_position.created_at,
            tbl_position.updated_at
        from {{ ref("l2_br_match_overrides") }} as tbl_override
        inner join
            {{ ref("int__enhanced_position") }} as tbl_position
            on tbl_override.br_database_id = tbl_position.br_database_id
        inner join
            {{ ref("m_election_api__district") }} as tbl_district
            on tbl_override.state = tbl_district.state
            and tbl_override.l2_district_type = tbl_district.l2_district_type
            and tbl_override.l2_district_name = tbl_district.l2_district_name
        where
            tbl_override.br_database_id not in (
                select br_database_id
                from {{ ref("stg_model_predictions__llm_l2_br_match_20260126") }}
                where br_database_id is not null
            )
    -- No incremental filter: re-emit every run so seed edits always propagate.
    ),

    unmatched_br_positions as (
        select
            tbl_position.id as id,
            tbl_position.br_database_id,
            tbl_position.br_position_id as br_position_id,
            tbl_position.name,
            tbl_position.state,
            tbl_position.level,
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
            and tbl_position.br_database_id not in (
                select br_database_id
                from override_injected_positions
                where br_database_id is not null
            )
            {% if is_incremental() %}
                and tbl_position.updated_at > (select max(updated_at) from {{ this }})
            {% endif %}
    ),

    all_positions as (
        select
            id,
            br_database_id,
            br_position_id,
            name,
            state,
            level,
            district_id,
            created_at,
            updated_at
        from matched_positions
        union all
        select
            id,
            br_database_id,
            br_position_id,
            name,
            state,
            level,
            district_id,
            created_at,
            updated_at
        from override_injected_positions
        union all
        select
            id,
            br_database_id,
            br_position_id,
            name,
            state,
            level,
            district_id,
            created_at,
            updated_at
        from unmatched_br_positions
    )

-- ICP win/serve are office-eligibility flags joined from int__icp_offices on
-- the BallotReady position id. Intentionally nullable (null = the office's
-- voter_count is unknown). Not date-gated: a position is not tied to a single
-- election, so consumers gate per-race if they need that.
select
    all_positions.id,
    all_positions.br_database_id,
    all_positions.br_position_id,
    all_positions.name,
    all_positions.state,
    all_positions.level,
    all_positions.district_id,
    all_positions.created_at,
    all_positions.updated_at,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    -- Free-text office compensation (BallotReady api_position.salary); powers the
    -- profile "About Office" salary row for sitting EOs with no active candidacy.
    br_position.salary
from all_positions
left join
    {{ ref("int__icp_offices") }} as icp
    on all_positions.br_database_id = icp.br_database_position_id
left join
    {{ ref("stg_airbyte_source__ballotready_api_position") }} as br_position
    on all_positions.br_database_id = br_position.database_id
