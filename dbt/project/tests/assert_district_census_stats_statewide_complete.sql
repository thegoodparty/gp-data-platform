-- Statewide completeness (DATA-1994): one statewide row per US state + DC, matching
-- the fips_codes state seed (50 + DC = 51; no Puerto Rico -- no L2 coverage).
-- Self-references the seed rather than hardcoding 51. Returns a row (fails) iff the
-- statewide count differs from the seed's state count.
select m.n as statewide_rows, s.n as fips_state_rows
from
    (
        select count(*) as n
        from {{ ref("district_census_stats") }}
        where district_type = 'State'
    ) m
cross join (select count(*) as n from {{ ref("fips_codes") }} where level = 'state') s
where m.n <> s.n
