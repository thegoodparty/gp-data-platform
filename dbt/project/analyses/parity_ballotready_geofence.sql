-- Parity for int__ballotready_geofence: new SQL model against the Python-built table.
--
-- Row-count equality is the wrong bar. The Python model accumulated coverage
-- over months of API calls; the new one reflects what the extraction DAG has
-- landed. Where the two disagree on a shared id, the question is which side
-- is stale, not which is broken.
--
-- Run with: dbt compile --select parity_ballotready_geofence, then execute
-- the compiled SQL.
with
    new_model as (select * from {{ ref("int__ballotready_geofence") }}),

    old_model as (select * from goodparty_data_catalog.dbt.int__ballotready_geofence),

    both as (
        select
            n.id,
            n.updated_at as new_updated_at,
            o.updated_at as old_updated_at,
            n.database_id = o.database_id
            and n.geo_id <=> o.geo_id
            and n.mtfcc <=> o.mtfcc
            and n.created_at <=> o.created_at
            and n.updated_at <=> o.updated_at
            and n.valid_from <=> o.valid_from
            and n.valid_to <=> o.valid_to as all_columns_match
        from new_model n
        inner join old_model o on n.id = o.id
    )

select
    (select count(*) from new_model) as new_rows,
    (select count(*) from old_model) as old_rows,
    (select count(*) from both) as shared_ids,
    (select count(*) from both where all_columns_match) as matching,
    -- The old side fetched earlier, so BallotReady may have changed the record since.
    -- Expected, not a defect.
    (
        select count(*)
        from both
        where not all_columns_match and old_updated_at < new_updated_at
    ) as old_side_stale,
    -- Same updated_at, different values: the transform disagrees with the Python model.
    -- This is the number that must be zero before cutover.
    (
        select count(*)
        from both
        where not all_columns_match and old_updated_at <=> new_updated_at
    ) as transform_bugs,
    (
        select count(*)
        from new_model
        where id not in (select id from old_model where id is not null)
    ) as only_new,
    (
        select count(*)
        from old_model
        where id not in (select id from new_model where id is not null)
    ) as only_old
