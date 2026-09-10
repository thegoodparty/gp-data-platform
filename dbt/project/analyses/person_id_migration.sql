-- Old vs new gp_person_id per person-graph record, for reviewing a person
-- vintage before it is swapped live or a mint change before it is merged.
--
-- `new` is this target's int__civics_person_canonical_ids; `old` defaults to
-- production's. Override with --vars '{person_id_migration_old: catalog.schema.table}'.
--
-- change_type per record:
-- unchanged    same id before and after
-- adopted      the record now carries an id that already existed (a merge)
-- reminted     the record now carries an id that did not exist before (a split)
-- new_record   not in the old mint at all
-- gone         not in the new mint at all
-- old_id_fate says whether the record's old id survives anywhere in the new
-- mint. The retired-id list is distinct (old_gp_person_id, new_gp_person_id)
-- where old_id_fate = 'retired'.
{% set old_relation = var(
    "person_id_migration_old",
    "goodparty_data_catalog.dbt.int__civics_person_canonical_ids",
) %}
with
    old as (select record_key, source_name, gp_person_id from {{ old_relation }}),

    new as (
        select record_key, source_name, gp_person_id
        from {{ ref("int__civics_person_canonical_ids") }}
    ),

    old_ids as (select distinct gp_person_id from old),

    new_ids as (select distinct gp_person_id from new),

    paired as (
        select
            coalesce(n.record_key, o.record_key) as record_key,
            coalesce(n.source_name, o.source_name) as source_name,
            o.gp_person_id as old_gp_person_id,
            n.gp_person_id as new_gp_person_id
        from old as o
        full outer join new as n using (record_key)
    )

select
    p.record_key,
    p.source_name,
    p.old_gp_person_id,
    p.new_gp_person_id,
    case
        when p.old_gp_person_id is null
        then 'new_record'
        when p.new_gp_person_id is null
        then 'gone'
        when p.old_gp_person_id = p.new_gp_person_id
        then 'unchanged'
        when oi.gp_person_id is not null
        then 'adopted'
        else 'reminted'
    end as change_type,
    case
        when p.old_gp_person_id is null
        then null
        when survives.gp_person_id is not null
        then 'survives'
        else 'retired'
    end as old_id_fate
from paired as p
left join old_ids as oi on oi.gp_person_id = p.new_gp_person_id
left join new_ids as survives on survives.gp_person_id = p.old_gp_person_id
