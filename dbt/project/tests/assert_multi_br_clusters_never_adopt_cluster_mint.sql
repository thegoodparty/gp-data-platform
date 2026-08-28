-- Row-preservation guard for the sole-BR-member adoption rule. The guard
-- fails silently if lost (the id dedupe deletes rows instead of duplicating
-- them, so unique tests cannot catch it), so assert it directly:
--
-- 1. BR election stages in multi-BR clusters must carry their self-mint, not
-- the shared cluster mint.
-- 2. No gp_candidacy_id in the BR candidacy-stage model may span two
-- (br_candidate_id, br_position_id, year) candidacy groups — the merge a
-- shared multi-BR cluster mint would produce.
select 'election_stage' as grain, f.br_race_id as violating_key
from {{ ref("int__civics_election_stage_ballotready") }} as f
inner join
    {{ ref("int__civics_minted_election_stage_ids") }} as m
    on m.source_name = 'ballotready'
    and m.source_id = f.br_race_id
where
    m.cluster_br_members > 1
    and f.gp_election_stage_id
    != {{
        generate_salted_uuid(
            fields=["'ballotready'", "f.br_race_id"], salt="election_stage"
        )
    }}

union all

select 'candidacy', gp_candidacy_id
from
    (
        select distinct
            mo.gp_candidacy_id,
            s.br_candidate_id,
            s.br_position_id,
            year(s.election_day) as election_year
        from {{ ref("int__civics_candidacy_stage_ballotready") }} as mo
        inner join
            {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }} as s
            on cast(s.br_candidacy_id as string) = mo.br_candidacy_id
    )
group by gp_candidacy_id
having count(*) > 1
