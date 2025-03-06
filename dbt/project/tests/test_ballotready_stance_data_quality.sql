-- Test to verify all stances have valid issue information
-- Returns records that fail the test (no records = test passes)
with stance_data as (
    select
        candidacy_id,
        stances
    from {{ ref('int__ballotready_stance') }}
),

exploded_stances as (
    select
        candidacy_id,
        stance
    from stance_data
    lateral view explode(stances) as stance
),

invalid_stances as (
    select
        candidacy_id,
        stance.databaseId as stance_id,
        stance.issue.databaseId as issue_id
    from exploded_stances
    where
        -- Check for stances without a valid issue ID
        stance.issue.databaseId is null or
        stance.issue.id is null
)

select * from invalid_stances
