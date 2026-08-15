-- Opponent-count inputs for the viability scorer from BallotReady race
-- rosters, selected by roster MEMBERSHIP: the candidacy's own BR candidacy
-- ids (all of its identity-mart stage rows) found inside the candidacies
-- array of exactly one clean race. Deliberately NOT the seats fallback's
-- race pick: matched_br_race_id is a position+date min-pick and is not
-- roster-verified, so it must never select the roster to count. Fail-closed:
-- membership in more than one clean race, an office-type contradiction
-- against the roster race's position, or a roster smaller than two members
-- produces NO row rather than a guess (a one-member roster usually means
-- BallotReady has not discovered the field yet -- the same distrust of a
-- count of 1 the scorer applies to its native per-election count). The
-- scorer consumes the pair (fallback_n_candidates, fallback_race_seats) for
-- log_n_losers where the native computation is missing, and the seats as
-- the last-resort multi_seat arm; count and seats come from the same race
-- row so the feature never mixes grains.
with
    stage_ids as (
        select gp_candidacy_id, cast(br_candidacy_id as string) as br_candidacy_id
        from {{ ref("candidacy_stage") }}
        where br_candidacy_id is not null and gp_candidacy_id is not null
        group by gp_candidacy_id, br_candidacy_id
    ),

    clean_races as (
        select
            race.database_id as br_race_id,
            race.candidacies,
            size(race.candidacies) as roster_size,
            race.seats,
            race.position.databaseid as br_position_database_id
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as race
        where {{ clean_general_race_conditions("race") }}
        -- One row per race: staging is raw and could someday carry several
        -- versions of a race; roster and seats must come from the same
        -- (latest) version, never a mix.
        qualify
            row_number() over (
                partition by race.database_id order by race.updated_at desc
            )
            = 1
    ),

    members as (
        select
            clean_races.br_race_id,
            clean_races.roster_size,
            clean_races.seats,
            clean_races.br_position_database_id,
            cast(member.databaseid as string) as br_candidacy_id
        from clean_races
        lateral view explode(clean_races.candidacies) exploded as member
    ),

    -- Office type recomputed from the candidacy's office-name string: the one
    -- signal independent of both the position crosswalk and the identity
    -- linkage, so it can veto an identity mis-cluster into another office's
    -- race (the failure membership cannot self-detect).
    keyed_candidacies as (
        select
            gp_candidacy_id,
            {{ map_office_type("candidate_office") }} as name_office_type
        from {{ ref("candidacy") }}
    ),

    crosswalk as (
        select br_position_database_id, office_type as crosswalk_office_type
        from {{ ref("int__civics_position_office_type") }}
    ),

    resolved as (
        select
            stage_ids.gp_candidacy_id,
            count(distinct members.br_race_id) as n_races,
            min(members.br_race_id) as br_race_id,
            min(members.roster_size) as roster_size,
            min(members.seats) as seats,
            max(
                case
                    when
                        crosswalk.crosswalk_office_type is not null
                        and keyed_candidacies.name_office_type is not null
                        and crosswalk.crosswalk_office_type <> 'Other'
                        and keyed_candidacies.name_office_type <> 'Other'
                        and crosswalk.crosswalk_office_type
                        <> keyed_candidacies.name_office_type
                        -- Town and City council straddle one municipal-council
                        -- taxonomy line between the name mapping and the
                        -- position crosswalk; the pair dominated the gate's
                        -- rejections while carrying no identity signal
                        -- (measured 2026-08-14). Compatible, not contradictory.
                        and not (
                            crosswalk.crosswalk_office_type
                            in ('City Council', 'Town Council')
                            and keyed_candidacies.name_office_type
                            in ('City Council', 'Town Council')
                        )
                    then 1
                    else 0
                end
            ) as any_office_contradiction
        from stage_ids
        inner join members on stage_ids.br_candidacy_id = members.br_candidacy_id
        inner join
            keyed_candidacies
            on stage_ids.gp_candidacy_id = keyed_candidacies.gp_candidacy_id
        left join
            crosswalk
            on members.br_position_database_id = crosswalk.br_position_database_id
        group by stage_ids.gp_candidacy_id
    )

-- With n_races = 1 the min() aggregates are simply THE race's values; several
-- member rows can exist only when one candidacy carries several BR ids inside
-- the same roster.
select
    gp_candidacy_id,
    roster_size as fallback_n_candidates,
    seats as fallback_race_seats,
    cast(br_race_id as string) as roster_br_race_id
from resolved
where n_races = 1 and roster_size >= 2 and any_office_contradiction = 0
