-- Canonical gp_person_id per person record. One row per
-- int__er_prematch_people unique_id (the record_key). The person group is the
-- cluster matcha published in er_source.clustered_people; a record the vintage
-- has not seen stands alone under its own key until the next run.
--
-- The id is minted from the group member earliest by first_seen_at, ties
-- broken by (source_name, source_id): first-in wins uniformly across sources,
-- so the id is stable when a later record (e.g. a BR row) joins a group minted
-- from a gp_api user. first_seen_at rides the prematch row, where each
-- record's native id and source timestamp share a row.
with
    published as (
        select unique_id, cluster_id, identity_id
        from {{ ref("stg_er_source__clustered_people") }}
    ),

    records as (
        select
            p.unique_id as record_key,
            p.source_name,
            p.source_id,
            coalesce(c.cluster_id, p.unique_id) as person_group_key,
            coalesce(c.identity_id, p.unique_id) as identity_key,
            p.first_seen_at
        from {{ ref("int__er_prematch_people") }} as p
        left join published as c using (unique_id)
    ),

    -- How many deterministic identities the published clustering fused; 1
    -- means nothing merged.
    identity_counts as (
        select person_group_key, count(distinct identity_key) as identity_count
        from records
        group by 1
    ),

    -- Earliest member mints the id. nulls last keeps a stray missing timestamp
    -- from hijacking a mint; the first_seen_at not_null test surfaces the gap.
    minting_member as (
        select
            person_group_key,
            source_name as minting_source_name,
            source_id as minting_source_id,
            count(*) over (partition by person_group_key) as group_size
        from records
        qualify
            row_number() over (
                partition by person_group_key
                order by first_seen_at asc nulls last, source_name asc, source_id asc
            )
            = 1
    )

select
    r.record_key,
    r.source_name,
    r.person_group_key,
    r.identity_key,
    ic.identity_count,
    r.first_seen_at,
    m.minting_source_name,
    m.minting_source_id,
    m.group_size,
    {{
        generate_salted_uuid(
            fields=["m.minting_source_name", "m.minting_source_id"], salt="person"
        )
    }} as gp_person_id
from records as r
inner join minting_member as m using (person_group_key)
inner join identity_counts as ic using (person_group_key)
