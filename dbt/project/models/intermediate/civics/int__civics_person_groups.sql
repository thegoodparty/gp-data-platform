-- Person groups. One row per record_key with its person_group_key: the min
-- record_key reachable over the non-conflicting edge list, via min-label
-- propagation across a fixed number of unrolled passes (Spark SQL, no
-- recursion). Labels stop changing after pass 5 on current data (pass 6 ==
-- pass 5, verified); 15 passes leaves headroom and the convergence test
-- (penultimate == final) proves it. See canonical-person-plan.md decision 1.
{% set passes = 15 %}
with
    -- Node universe: every source record participating in person identity.
    -- Superset of all edge endpoints (edges join the same source tables).
    nodes as (
        select distinct 'ballotready|' || cast(br_candidate_id as string) as record_key
        from {{ ref("stg_airbyte_source__ballotready_s3_candidacies_v3") }}
        where br_candidate_id is not null
        union
        select distinct 'ballotready|' || cast(br_candidate_id as string)
        from {{ ref("stg_airbyte_source__ballotready_s3_office_holders_v3") }}
        where br_candidate_id is not null
        union
        select 'gp_api|' || cast(id as string)
        from {{ ref("stg_airbyte_source__gp_api_db_user") }}
        union
        select 'hubspot|' || cast(id as string)
        from {{ ref("stg_airbyte_source__hubspot_api_contacts") }}
        union
        select distinct 'techspeed_officeholder|' || cast(ts_officeholder_id as string)
        from {{ ref("int__civics_elected_official_canonical_ids") }}
        where not ts_officeholder_id_is_reused
        union
        select distinct source_name || '|' || source_id
        from {{ ref("stg_er_source__clustered_candidacy_stages") }}
        where source_name in ('techspeed', 'ddhq')
    ),

    -- Undirected adjacency with a self-loop per node, conflict edges excluded.
    -- The self-loop lets each pass take min() over neighbors only (own label
    -- included as a neighbor), so it references the prior pass exactly once —
    -- a linear chain, not the exponential self+neighbor double reference.
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

    labels_0 as (select record_key, record_key as person_group_key from nodes),

    {% for n in range(1, passes + 1) %}
        labels_{{ n }} as (
            select a.src as record_key, min(l.person_group_key) as person_group_key
            from adjacency as a
            inner join labels_{{ n - 1 }} as l on l.record_key = a.dst
            group by a.src
        ),
    {% endfor %}

    -- Groups touched by any conflict edge (E7 keys spanning >1 br person).
    -- Both endpoints: conflict edges are excluded from propagation, so the
    -- two sides land in different groups and each group needs the flag.
    conflict_endpoints as (
        select record_key_1 as record_key
        from {{ ref("int__civics_person_edges") }}
        where is_conflict
        union
        select record_key_2
        from {{ ref("int__civics_person_edges") }}
        where is_conflict
    ),

    conflict_groups as (
        select distinct f.person_group_key
        from conflict_endpoints as ce
        inner join labels_{{ passes }} as f using (record_key)
    )

select
    f.record_key,
    substring_index(f.record_key, '|', 1) as source_name,
    f.person_group_key,
    prev.person_group_key as pass_penultimate_key,
    cg.person_group_key is not null as had_conflict
from labels_{{ passes }} as f
inner join labels_{{ passes - 1 }} as prev using (record_key)
left join conflict_groups as cg on cg.person_group_key = f.person_group_key
