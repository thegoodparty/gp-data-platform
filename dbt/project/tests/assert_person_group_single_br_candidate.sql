-- At most one distinct br_candidate_id per person group, with no tolerance.
-- Two distinct BallotReady people in one group means gp_person_id claims two
-- people are one, which is the claim the whole design rests on not making.
--
-- Reads the mint's view of the published groups, so it also covers a record
-- the vintage never saw and a vintage that drifted from tonight's sources.
-- matcha enforces the rule twice (an identity reaching two BR people
-- dissolves, a merge that would fuse two is refused); this proves neither
-- leaks into what dbt publishes.
with
    br_records as (
        select person_group_key, substring_index(record_key, '|', -1) as br_candidate_id
        from {{ ref("int__civics_person_canonical_ids") }}
        where source_name = 'ballotready'
    )

select person_group_key, count(distinct br_candidate_id) as distinct_br
from br_records
group by person_group_key
having count(distinct br_candidate_id) > 1
