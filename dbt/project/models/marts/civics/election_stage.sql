-- Civics mart election_stage table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
select
    gp_election_stage_id,
    gp_election_id,

    -- Vendor identifiers
    cast(null as string) as br_race_id,
    cast(null as string) as br_election_id,
    cast(null as string) as br_position_id,
    ddhq_race_id,

    -- Stage info
    election_stage as stage_type,
    ddhq_election_stage_date as election_date,
    cast(null as string) as election_name,
    ddhq_race_name as race_name,

    -- Stage type flags
    election_stage = 'primary' as is_primary,
    election_stage = 'runoff' as is_runoff,
    cast(null as boolean) as is_retention,

    -- Election details
    cast(null as int) as number_of_seats,
    total_votes_cast,

    -- ICP flags
    cast(null as boolean) as is_win_icp,
    cast(null as boolean) as is_serve_icp,

    -- Timestamps
    created_at,
    cast(null as timestamp) as updated_at

from {{ ref("int__civics_election_stage_2025") }}
