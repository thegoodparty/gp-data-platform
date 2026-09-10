-- Person identities. One row per record_key with the identity it belongs to:
-- the connected component over the non-conflicting links, via min-label
-- propagation across 15 unrolled passes (Spark SQL, no recursion). Closing
-- over links is sound because native identifiers are transitive by definition,
-- and E5's one detectable exception -- a candidacy cluster spanning two
-- BallotReady people -- is suppressed upstream.
--
-- This is also the pregroup source for the Splink person entity, and it reads
-- no similarity edges at all, so by DAG topology a published match can never
-- feed the next run's pregroups.
--
-- An identity holds at most one BallotReady person. A component can still
-- reach two along a path of individually sound links -- BR1 by a HubSpot
-- contact's candidacy id, that contact to its gp_api user by foreign key, that
-- user to BR2 by the elected-official bridge -- which means one of the source
-- records is simply wrong. Those components dissolve: every member becomes its
-- own identity, and the similarity tier may reattach them on direct evidence.
-- Keeping the lexically smaller BR id, which plain propagation does, would
-- assert one of two contradictory identities at even odds.
{% set passes = 15 %}
with
    nodes as (
        select record_key, source_name from {{ ref("int__civics_person_nodes") }}
    ),

    adjacency as (
        select record_key_1 as src, record_key_2 as dst
        from {{ ref("int__civics_person_links") }}
        where not is_conflict
        union all
        select record_key_2, record_key_1
        from {{ ref("int__civics_person_links") }}
        where not is_conflict
        union all
        select record_key, record_key
        from nodes
    ),

    {{ min_label_propagation("adjacency", "nodes", passes) }},

    labelled as (
        select f.record_key, n.source_name, f.person_group_key as component_key
        from labels_{{ passes }} as f
        inner join nodes as n using (record_key)
    ),

    contested as (
        select component_key
        from labelled
        where source_name = 'ballotready'
        group by component_key
        having count(distinct record_key) > 1
    )

select
    l.record_key,
    l.source_name,
    case
        when c.component_key is null then l.component_key else l.record_key
    end as identity_key,
    c.component_key is not null as br_contested,
    l.component_key as pass_final_key,
    prev.person_group_key as pass_penultimate_key
from labelled as l
inner join labels_{{ passes - 1 }} as prev using (record_key)
left join contested as c on c.component_key = l.component_key
