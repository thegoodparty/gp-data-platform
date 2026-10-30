-- Estimated opponent count per election, derived by counting the active
-- candidacies loaded within an election-stage. Feeds a contested-direction
-- fallback for number_of_opponents / is_uncontested on the election mart, which
-- both come only from TechSpeed/DDHQ today (BallotReady hardcodes them NULL).
--
-- One-directional by design: a stage with >= 2 active candidates yields >= 1
-- opponent. A field of <= 1 means we have not loaded the rest of the roster, NOT
-- that the race is uncontested, so it stays NULL. We never assert 0 opponents.
--
-- is_roster_contested is the same evidence held against the seats: more active
-- candidates than seats, or a recorded loss or runoff. Named candidates on the
-- roster outrank a vendor's earlier count, which can predate the filing close.
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
            -- A person entity resolution missed shows up twice under one name,
            -- and would read as an opponent.
            least(
                count(distinct coalesce(cs.gp_person_id, cs.gp_candidacy_id)),
                count(
                    distinct coalesce(
                        lower(regexp_replace(trim(cs.candidate_name), '\\s+', ' ')),
                        cs.gp_candidacy_id
                    )
                )
            ) as active_candidate_count,
            -- Some multi-seat races are recorded with one seat; the winners show
            -- how many there were.
            greatest(
                coalesce(max(es.number_of_seats), 1),
                count_if(cs.election_result = 'Won')
            ) as seats,
            count_if(cs.election_result in ('Lost', 'Runoff')) > 0 as has_recorded_loss
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

    stage_contest as (
        select
            *,
            active_candidate_count > seats or has_recorded_loss as is_contested,
            stage_type in (
                'general', 'general special', 'general runoff', 'general special runoff'
            ) as is_deciding_stage
        from stage_candidates
    ),

    election_rollup as (
        select
            gp_election_id,
            -- deciding-contest field: the largest single general-family race.
            -- Includes runoff variants — a general/special runoff is a deciding
            -- contest too, and folding it in stops a runoff-only race from
            -- falling through to an earlier, larger primary field.
            max(
                case when is_deciding_stage then active_candidate_count end
            ) as general_field_size,
            max(active_candidate_count) as max_field_size,
            bool_or(
                case when is_deciding_stage then is_contested end
            ) as general_is_contested,
            bool_or(is_contested) as any_stage_is_contested
        from stage_contest
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
    end as estimated_opponents_stage_basis,
    -- Same stage choice as the field size. True only, never false: a roster at
    -- or under the seats may just be missing candidates.
    case
        when coalesce(general_is_contested, any_stage_is_contested) then true
    end as is_roster_contested
from election_rollup
