{{
    config(
        materialized="table",
    )
}}

-- Civics mart election_stage table
-- Sources from intermediate/civics archived data (elections on or before 2025-12-31)
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

from {{ ref("int__civics_election_stage_2025") }}
