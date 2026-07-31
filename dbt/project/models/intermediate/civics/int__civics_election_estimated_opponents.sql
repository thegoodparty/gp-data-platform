-- Estimated opponent count per election, derived by counting the active
-- candidacies loaded within an election-stage. Feeds a contested-direction
-- fallback for number_of_opponents / is_uncontested on the election mart, which
-- both come only from TechSpeed/DDHQ today (BallotReady hardcodes them NULL).
--
-- One-directional by design: a stage with >= 2 active candidates yields >= 1
-- opponent. A field of <= 1 means we have not loaded the rest of the roster, NOT
-- that the race is uncontested, so it stays NULL. We never assert 0 opponents.
--
-- Grain: one row per gp_election_id. Counting per stage instance (not per
-- gp_election_id) keeps a single election id that spans several offices from
-- pooling opponents across different races. The rollup prefers the general
-- (deciding) contest and falls back to the largest loaded stage only when no
-- general-stage candidacies exist.
with
    stage_candidates as (
        -- Active candidates per election-stage. "Active" = actually competed in
        -- the stage: drop Withdrew / Not on Ballot so a name that filed but never
        -- ran is not counted as an opponent. Losers stay counted — losing a stage
        -- still means they were on the ballot for it.
        select
            es.gp_election_id,
            es.gp_election_stage_id,
            es.stage_type,
            count(
                distinct coalesce(cs.gp_person_id, cs.gp_candidacy_id)
            ) as active_candidate_count
        from {{ ref("candidacy_stage") }} as cs
        inner join
            {{ ref("election_stage") }} as es
            on cs.gp_election_stage_id = es.gp_election_stage_id
        where
            es.gp_election_id is not null
            and (
                cs.election_result is null
                or cs.election_result not in ('Withdrew', 'Not on Ballot')
            )
        group by es.gp_election_id, es.gp_election_stage_id, es.stage_type
    ),

    election_rollup as (
        select
            gp_election_id,
            -- deciding-contest field: the largest single general-stage race
            max(
                case
                    when stage_type in ('general', 'general special')
                    then active_candidate_count
                end
            ) as general_field_size,
            max(active_candidate_count) as max_field_size
        from stage_candidates
        group by gp_election_id
    )

select
    gp_election_id,
    -- prefer the general stage; fall back to the largest loaded stage
    coalesce(general_field_size, max_field_size) as estimated_field_size,
    case
        when estimated_field_size >= 2 then estimated_field_size - 1
    end as estimated_number_of_opponents,
    case
        when estimated_field_size < 2
        then null
        when general_field_size is not null
        then 'general'
        else 'largest_stage'
    end as estimated_opponents_stage_basis
from election_rollup
