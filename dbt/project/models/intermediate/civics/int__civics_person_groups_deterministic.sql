-- Deterministic-only person groups: connected components over the
-- deterministic edge model (native ids + candidacy-cluster co-membership),
-- never over probabilistic person-matcher output. This is the pregroup source
-- for the Splink person entity: matcha injects these groups as p=1.0 edges, so
-- by DAG topology a published probabilistic merge can never feed back into the
-- next run's pregroups. Identical to int__civics_person_groups until a
-- probabilistic person edge model joins that model's adjacency.
{% set passes = 15 %}
with
    nodes as (
        select record_key, source_name from {{ ref("int__civics_person_nodes") }}
    ),

    adjacency as (
        select record_key_1 as src, record_key_2 as dst
        from {{ ref("int__civics_person_edges") }}
        where not is_conflict
        union all
        select record_key_2, record_key_1
        from {{ ref("int__civics_person_edges") }}
        where not is_conflict
        union all
        select record_key, record_key
        from nodes
    ),

    {{ min_label_propagation("adjacency", "nodes", passes) }}

select
    f.record_key,
    n.source_name,
    f.person_group_key as deterministic_group_key,
    prev.person_group_key as pass_penultimate_key
from labels_{{ passes }} as f
inner join nodes as n using (record_key)
inner join labels_{{ passes - 1 }} as prev using (record_key)
