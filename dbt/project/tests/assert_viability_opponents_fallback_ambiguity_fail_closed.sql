-- Membership ambiguity must fail closed: a candidacy whose stage ids appear
-- in the rosters of MORE THAN ONE clean race gets no fallback row, even when
-- the rosters happen to agree on counts. Recomputes membership with an
-- inline clean-race predicate, deliberately independent of the model and the
-- shared macro under test. Returns violating rows; empty = pass.
with
    stage_ids as (
        select gp_candidacy_id, cast(br_candidacy_id as string) as br_candidacy_id
        from {{ ref("candidacy_stage") }}
        where br_candidacy_id is not null and gp_candidacy_id is not null
        group by gp_candidacy_id, br_candidacy_id
    ),

    members as (
        select
            race.database_id as br_race_id,
            cast(member.databaseid as string) as br_candidacy_id
        from {{ ref("stg_airbyte_source__ballotready_api_race") }} as race
        lateral view explode(race.candidacies) exploded as member
        where
            race.is_disabled = false
            and race.is_recall = false
            and race.is_primary = false
            and race.is_runoff = false
            and race.is_unexpired = false
            and race.seats > 0
    ),

    multi_race as (
        select stage_ids.gp_candidacy_id
        from stage_ids
        inner join members on stage_ids.br_candidacy_id = members.br_candidacy_id
        group by stage_ids.gp_candidacy_id
        having count(distinct members.br_race_id) > 1
    )

select fallback.gp_candidacy_id
from {{ ref("int__civics_viability_opponents_fallback") }} as fallback
inner join multi_race on fallback.gp_candidacy_id = multi_race.gp_candidacy_id
