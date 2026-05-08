-- Every non-null candidacy.general_election_result must equal the
-- election_result of some general-stage row in candidacy_stage for that
-- candidacy. Catches regressions in the candidacy.sql left-join that would
-- otherwise drift from the source-of-truth stage data. Zero rows expected.
select c.gp_candidacy_id, c.general_election_result
from {{ ref("candidacy") }} as c
where
    c.general_election_result is not null
    and not exists (
        select 1
        from {{ ref("candidacy_stage") }} as cs
        join
            {{ ref("election_stage") }} as es
            on cs.gp_election_stage_id = es.gp_election_stage_id
        where
            cs.gp_candidacy_id = c.gp_candidacy_id
            and lower(es.stage_type) = 'general'
            and cs.election_result = c.general_election_result
    )
