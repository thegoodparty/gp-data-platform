-- Person groups. One row per record_key with its person_group_key: the
-- record's identity, coarsened by the similarity merges admitted here.
--
-- A set of identities merges only when every pair inside it appears in
-- int__civics_person_similarities, and when the result holds at most one
-- BallotReady person. Transitive closure over similarities is what fused two
-- BallotReady people 6,219 times, and it discards the matcher's own pairwise
-- guarantees: it emits no BR-to-BR pair, yet chaining through a shared
-- neighbour fuses them anyway. Anything short of complete support stays split,
-- which is the intended trade -- a false negative costs a duplicate profile, a
-- false positive merges two people's HubSpot contacts.
--
-- No propagation. A completely supported group is exactly a set of identities
-- that all share one closed neighbourhood whose size equals the set's own, so
-- grouping on the neighbourhood array finds them in a single aggregation.
-- Proof: if every member of a group has closed neighbourhood S and the group
-- has |S| members, then the group is S (each member is in its own
-- neighbourhood), so every pair in S is adjacent and no member has a
-- neighbour outside S.
with
    identities as (
        select record_key, source_name, identity_key, br_contested
        from {{ ref("int__civics_person_identities") }}
    ),

    identity_stats as (
        select
            identity_key,
            count(
                distinct case when source_name = 'ballotready' then record_key end
            ) as br_count
        from identities
        group by 1
    ),

    undirected as (
        select identity_key_1 as identity_key, identity_key_2 as neighbour
        from {{ ref("int__civics_person_similarities") }}
        union all
        select identity_key_2, identity_key_1
        from {{ ref("int__civics_person_similarities") }}
        union all
        -- Self-membership, so an identity with no similar neighbour gets a
        -- neighbourhood of one and falls out as its own person.
        select identity_key, identity_key
        from identity_stats
    ),

    neighbourhoods as (
        select identity_key, sort_array(collect_set(neighbour)) as closed_neighbourhood
        from undirected
        group by identity_key
    ),

    -- Identities proposing the same merge. A proposal is completely supported
    -- only when as many identities propose it as it names.
    proposals as (
        select
            n.closed_neighbourhood,
            count(*) as proposing_identities,
            size(n.closed_neighbourhood) as named_identities,
            sum(s.br_count) as br_count,
            array_min(n.closed_neighbourhood) as group_key
        from neighbourhoods as n
        inner join identity_stats as s using (identity_key)
        group by n.closed_neighbourhood
    ),

    verdicts as (
        select
            n.identity_key,
            case
                when p.br_count > 1
                then 'br_cannot_link'
                when p.proposing_identities < p.named_identities
                then 'incomplete_support'
            end as rejected_reason,
            p.named_identities,
            p.group_key
        from neighbourhoods as n
        inner join proposals as p using (closed_neighbourhood)
    ),

    -- A refused merge leaves every identity as its own person. Both branches
    -- are the min record_key of the resulting group, so the label keeps the
    -- meaning the identity tier gives it and the id mint is unchanged.
    resolved as (
        select
            i.record_key,
            i.source_name,
            i.identity_key,
            i.br_contested,
            case
                when v.rejected_reason is null then v.group_key else i.identity_key
            end as person_group_key,
            case
                when v.rejected_reason is null then v.named_identities else 1
            end as identity_count,
            v.rejected_reason
        from identities as i
        inner join verdicts as v using (identity_key)
    ),

    -- Groups whose evidence included a contradiction that was suppressed
    -- upstream: a reused vendor key, or an identity that dissolved because it
    -- reached two BallotReady people.
    conflict_endpoints as (
        select record_key_1 as record_key
        from {{ ref("int__civics_person_links") }}
        where is_conflict
        union
        select record_key_2
        from {{ ref("int__civics_person_links") }}
        where is_conflict
    ),

    conflict_groups as (
        select distinct r.person_group_key
        from resolved as r
        left join conflict_endpoints as ce using (record_key)
        where r.br_contested or ce.record_key is not null
    )

select
    r.record_key,
    r.source_name,
    r.person_group_key,
    r.identity_key,
    r.identity_count,
    r.rejected_reason,
    cg.person_group_key is not null as had_conflict
from resolved as r
left join conflict_groups as cg on cg.person_group_key = r.person_group_key
