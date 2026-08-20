-- Silent-drop tripwire: the resolver must emit rows for nearly all
-- candidacies whose stage ids sit in at least one clean race with a roster
-- of two or more (an independently recomputed upper bound that ignores the
-- model's fail-closed gates). Counts the INTERSECTION of expected and
-- emitted keys, so emitting wrong keys cannot mask dropping expected ones.
-- A share floor, not a fixed count: the small measured gate-rejection rate
-- and BR roster drift never trip it on their own; a resolver bug dropping a
-- material share does. Fails too when the bound is empty (upstream break).
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
            size(race.candidacies) as roster_size,
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

    upper_bound as (
        select stage_ids.gp_candidacy_id
        from stage_ids
        inner join members on stage_ids.br_candidacy_id = members.br_candidacy_id
        where members.roster_size >= 2
        group by stage_ids.gp_candidacy_id
    )

select
    count(*) as upper_bound_rows,
    count_if(fallback.gp_candidacy_id is not null) as emitted_expected_rows
from upper_bound
left join
    {{ ref("int__civics_viability_opponents_fallback") }} as fallback
    on upper_bound.gp_candidacy_id = fallback.gp_candidacy_id
having
    count(*) = 0
    or count_if(fallback.gp_candidacy_id is not null) * 1.0 / nullif(count(*), 0) < 0.95
