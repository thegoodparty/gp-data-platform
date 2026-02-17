-- Civics mart election_stage table
-- Union of 2025 HubSpot archive and 2026+ BallotReady data
with
    combined as (
        select *
        from {{ ref("int__civics_election_stage_2025") }}
        union all
        select *
        from {{ ref("int__civics_election_stage_ballotready") }}
    ),

    deduplicated as (
        select *
        from combined
        qualify
            row_number() over (
                partition by gp_election_stage_id order by created_at desc
            )
            = 1
    )

select
    gp_election_stage_id,
    gp_election_id,
    hubspot_contact_id,
    ddhq_race_id,
    election_stage,
    ddhq_election_stage_date,
    ddhq_race_name,
    total_votes_cast,
    created_at

from deduplicated
