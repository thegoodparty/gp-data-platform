-- Parity for int__ballotready_geofence: new SQL model against the Python-built table.
--
-- Must be run with `--target dev`. `new_model` resolves through `ref()`, but
-- `old_model` is the hardcoded production relation, so on `--target prod` the two
-- refs point at the same object: the harness compares the new view against itself
-- and every count comes back as perfect, meaningless agreement.
--
-- The comparison is hand-rolled with `<=>` chains rather than audit_helper's
-- compare_all_columns, which is installed here: that macro reports mismatched rows,
-- not why they mismatch, and this harness needs the old-side-stale versus
-- transform-bug split below to tell a stale API snapshot from an actual regression.
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
    -- Arithmetic residual, not another predicate: old_updated_at > new_updated_at (old
    -- side newer, e.g. mid-backfill) and a null on only one side both fail every
    -- predicate above (< is UNKNOWN, <=> is false), so a predicate-based bucket would
    -- inherit the same null semantics that created the gap. Every non-matching row must
    -- land in exactly one bucket; non-zero here means the classifier missed a case and
    -- transform_bugs cannot be trusted until it is explained.
    shared_ids - matching - old_side_stale - transform_bugs as unclassified,
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
