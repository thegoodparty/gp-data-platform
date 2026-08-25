with
    -- Both the LLM match results and the override seed name a district by its
    -- L2 spelling, which L2 rewrites between vintages. Resolve through this so
    -- a stale spelling still lands on the district that carries the voters.
    resolved_districts as ({{ l2_district_spelling_resolution() }}),

    matched_positions as (
        select distinct
            tbl_position.id as id,
            tbl_match.br_database_id,
            tbl_position.br_position_id as br_position_id,
            tbl_position.name,
            -- The office's own state. The district's state (l2_state) is a
            -- join key below, never this output column: the two can diverge
            -- when an office changes state after matching.
            tbl_position.state,
            tbl_position.level,
            tbl_district.district_id,
            tbl_position.created_at,
            tbl_position.updated_at
        from {{ ref("stg_model_predictions__llm_l2_br_match") }} as tbl_match
        inner join
            {{ ref("int__enhanced_position") }} as tbl_position
            on tbl_match.br_database_id = tbl_position.br_database_id
        left join
            {{ ref("l2_br_match_overrides") }} as tbl_override
            on tbl_match.br_database_id = tbl_override.br_database_id
        left join
            resolved_districts as tbl_district
            on coalesce(tbl_override.state, tbl_match.l2_state) = tbl_district.state
            and coalesce(tbl_override.l2_district_type, tbl_match.l2_district_type)
            = tbl_district.l2_district_type
            and coalesce(tbl_override.l2_district_name, tbl_match.l2_district_name)
            = tbl_district.l2_district_name
        where
            -- null-safe: an unmatched row carries a null district name, which is not
            -- one of these non-office names. Bare NOT IN evaluates to NULL there and
            -- would drop the row, silently discarding any override that exists to
            -- give that very position a district.
            (
                tbl_match.l2_district_name is null
                or tbl_match.l2_district_name not in (
                    'County Committee Female Member',
                    'County Committee Male Member',
                    'President of the United States',
                    'Vice President of the United States'
                )
            )
            and tbl_district.district_id is not null
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
    ),

    -- Inject a match from the override seed for positions absent from the match
    -- table (the left join above can only correct rows that exist there).
    override_injected_positions as (
        select distinct
            tbl_position.id as id,
            tbl_override.br_database_id,
            tbl_position.br_position_id as br_position_id,
            tbl_position.name,
            -- Office state from the position side here too; the override's
            -- state stays the district join key below.
            tbl_position.state,
            tbl_position.level,
            tbl_district.district_id,
            tbl_position.created_at,
            tbl_position.updated_at
        from {{ ref("l2_br_match_overrides") }} as tbl_override
        inner join
            {{ ref("int__enhanced_position") }} as tbl_position
            on tbl_override.br_database_id = tbl_position.br_database_id
        inner join
            resolved_districts as tbl_district
            on tbl_override.state = tbl_district.state
            and tbl_override.l2_district_type = tbl_district.l2_district_type
            and tbl_override.l2_district_name = tbl_district.l2_district_name
        where
            tbl_override.br_database_id not in (
                select br_database_id
                from {{ ref("stg_model_predictions__llm_l2_br_match") }}
                where br_database_id is not null
            )
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
    -- string to match the election-api Postgres contract (Position.br_database_id
    -- is text); consumers joining numerically cast back explicitly
    cast(all_positions.br_database_id as string) as br_database_id,
    all_positions.br_position_id,
    all_positions.name,
    all_positions.state,
    all_positions.level,
    all_positions.district_id,
    all_positions.created_at,
    all_positions.updated_at,
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    -- Free-text BR compensation (ranges/notes); shown in About Office for
    -- sitting officials with no active candidacy.
    api_position.salary
from all_positions
left join
    {{ ref("int__icp_offices") }} as icp
    on all_positions.br_database_id = icp.br_database_position_id
left join
    {{ ref("stg_airbyte_source__ballotready_api_position") }} as api_position
    on all_positions.br_database_id = api_position.database_id
