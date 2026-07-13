-- API invariant (DATA-2107): every year the calendar serves Primary dates
-- for must also carry a complete General set (51 = 50 states + DC).
-- Without this, a year like 2028 would classify primary days as Primary
-- while its own November general falls through to LocalOrMunicipal (the
-- no-match default) and silently serves local-model numbers for general
-- races. This is also the loud tripwire for the seed's forward horizon:
-- when BallotReady starts publishing the next even cycle's primaries, this
-- fails until the seed gains that year's General rows.
select election_year, n_general, n_primary
from
    (
        select
            year(election_date) as election_year,
            count_if(election_code = 'General') as n_general,
            count_if(election_code = 'Primary') as n_primary
        from {{ ref("m_election_api__election_calendar") }}
        group by 1
    )
where n_primary > 0 and n_general < 51
