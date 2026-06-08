-- DATA-1950 (G5420): the boilerplate bug pinned many distinct districts onto one place
-- (93 -> Broome). Guard the recurrence: no (state, county_name, population) shared by
-- >= 12
-- distinct resolved G5420 districts. Measured max legit cluster = 7 (SC/WY numbered
-- county
-- districts, correct county); the old bug pinned 50-93, so 12 sits in the gap.
with
    g5420_resolved_rows as (
        select f.state, f.county_name, f.population, f.database_id
        from {{ ref("int__place_fast_facts") }} as f
        join
            {{ ref("stg_airbyte_source__ballotready_api_place") }} as p
            on p.database_id = f.database_id
        where p.mtfcc = 'G5420' and f.county_fips is not null
    )
select state, county_name, population, count(distinct database_id) as n_districts
from g5420_resolved_rows
group by state, county_name, population
having count(distinct database_id) >= 12
