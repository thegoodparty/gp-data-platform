-- Recompute each race's day type from first principles (the federal November
-- expression + the calendar model) and fail on any row whose stored
-- election_code disagrees. The recompute resolves the calendar state the same
-- way the mart's key CTE does (district's corrected state where a district
-- exists, else the race's own): this pins the derivation against wiring
-- regressions in the mart's join or CASE order.
with
    expected as (
        select
            tbl_race.id,
            case
                when
                    year(tbl_race.election_date) % 2 = 0
                    and cast(tbl_race.election_date as date) = date_add(
                        next_day(
                            make_date(year(tbl_race.election_date), 11, 1)
                            - interval 1 day,
                            'MON'
                        ),
                        1
                    )
                then 'General'
                when tbl_primary.state is not null
                then 'Primary'
                else 'LocalOrMunicipal'
            end as expected_code
        from {{ ref("m_election_api__race") }} as tbl_race
        left join
            {{ ref("m_election_api__position") }} as tbl_position
            on tbl_race.position_id = tbl_position.id
        left join
            {{ ref("m_election_api__district") }} as tbl_district
            on tbl_position.district_id = tbl_district.id
        left join
            {{ ref("int__election_calendar_primary_ballotready") }} as tbl_primary
            on coalesce(tbl_district.state, tbl_race.state) = tbl_primary.state
            and cast(tbl_race.election_date as date) = tbl_primary.election_date
    )

select tbl_race.id, tbl_race.election_code, expected.expected_code
from {{ ref("m_election_api__race") }} as tbl_race
inner join expected on tbl_race.id = expected.id
where tbl_race.election_code != expected.expected_code
