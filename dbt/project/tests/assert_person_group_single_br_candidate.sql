-- At most one distinct br_candidate_id per person group, with no tolerance.
-- Two distinct BallotReady people in one group means gp_person_id claims two
-- people are one, which is the claim the whole design rests on not making.
--
-- Reads the final graph, which covers the identity tier too: a person group is
-- a union of identities, so an identity holding two BR people also shows up
-- here. Both mechanisms are structural rather than tolerated -- an identity
-- that reaches two BR people dissolves, and a similarity merge that would fuse
-- two is refused. This test is what proves neither leaks.
with
    br_records as (
        select person_group_key, substring_index(record_key, '|', -1) as br_candidate_id
        from {{ ref("int__civics_person_groups") }}
        where source_name = 'ballotready'
    )

select person_group_key, count(distinct br_candidate_id) as distinct_br
from br_records
group by person_group_key
having count(distinct br_candidate_id) > 1
