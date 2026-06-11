-- Civics mart positions table.
-- Mostly pass-through over int__icp_offices: every BallotReady position we know
-- about, with its L2 district match, voter count, and ICP eligibility flags.
-- Standardized office_type is joined from int__civics_position_office_type,
-- keyed on the same br_position_database_id, so positions, election, and
-- candidacy marts share one office classification.
--
-- ICP flags here are position-level (un-gated): they reflect whether the office
-- type and district size qualify for the Win / Serve / Supersize ICP. The
-- election and candidacy marts apply an additional effective-date gate on top
-- of is_win_icp / is_win_supersize_icp; this model does not, since a position
-- has no election dates.
with
    icp as (select * from {{ ref("int__icp_offices") }}),

    position_office_type as (
        select * from {{ ref("int__civics_position_office_type") }}
    )

select
    icp.br_database_position_id as br_position_database_id,
    icp.br_position_id,
    icp.state,
    icp.br_position_name as position_name,

    -- Standardized office naming, shared with the election/candidacy marts
    position_office_type.office_type,
    icp.normalized_position_type as br_normalized_position_type,

    icp.is_judicial,
    icp.is_appointed,

    -- L2 district match
    icp.l2_district_name,
    icp.l2_district_type,
    icp.is_matched as is_l2_matched,
    icp.voter_count as icp_voter_count,

    -- ICP eligibility flags (position-level, not date-gated)
    icp.icp_office_win as is_win_icp,
    icp.icp_office_serve as is_serve_icp,
    icp.icp_win_supersize as is_win_supersize_icp,
    icp.icp_win_effective_date,

    icp.updated_at

from icp
left join
    position_office_type
    on icp.br_database_position_id = position_office_type.br_position_database_id
