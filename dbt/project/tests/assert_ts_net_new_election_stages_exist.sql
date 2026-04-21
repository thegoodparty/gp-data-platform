-- At least one TechSpeed-only election_stage must appear in the mart.
-- Validates the UNION ALL net-new logic is working — if this fails,
-- TechSpeed races not covered by BallotReady are being silently dropped.
select 1 as validation_error
where
    not exists (
        select 1
        from {{ ref("election_stage") }}
        where
            array_contains(source_systems, 'techspeed')
            and not array_contains(source_systems, 'ballotready')
    )
