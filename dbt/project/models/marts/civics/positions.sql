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

    -- Census constituents for the same district. Keeps the canonical name it
    -- carries everywhere else (int__district_population, district_census_stats)
    -- rather than an icp_ prefix, since the population is a property of the
    -- district and no ICP gate reads it.
    --
    -- Rounded to a whole person: the allocation is fractional upstream so that
    -- allocations sum exactly to block population (the mass-conservation
    -- invariant the rollups and binding tests rely on), but a leaf mart column
    -- that reads as a count of people should not carry a fractional tail. The
    -- exact value stays available on int__district_population.
    cast(round(icp.district_population) as bigint) as district_population,

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
