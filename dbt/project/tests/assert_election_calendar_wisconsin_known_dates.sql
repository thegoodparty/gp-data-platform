-- Wisconsin's fewest-races-wins exception is the calendar's most
-- junk-sensitive rule: a single stray low-count 'Primary'-named WI row
-- would flip the pick to itself. Pin the derivation to the August
-- partisan-primary dates confirmed against L2 ground truth for the three
-- completed even cycles; these are immutable historical facts, not
-- drifting thresholds.
with
    expected as (
        select *
        from
            (
                values
                    ('WI', date '2020-08-11'),
                    ('WI', date '2022-08-09'),
                    ('WI', date '2024-08-13')
            ) as t(state, election_date)
    )

select
    expected.state,
    expected.election_date as expected_date,
    cal.election_date as actual_date
from expected
left join
    {{ ref("int__election_calendar_primary_ballotready") }} as cal
    on expected.state = cal.state
    and year(expected.election_date) = year(cal.election_date)
where cal.election_date is null or cal.election_date != expected.election_date
