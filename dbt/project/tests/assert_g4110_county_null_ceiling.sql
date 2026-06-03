-- dbt/project/tests/assert_g4110_county_null_ceiling.sql
-- DATA-1950: G4110 places with no resolvable county should stay near the measured 247.
select count(*) as g4110_null_county
from {{ ref("int__place_fast_facts") }} as f
join
    {{ ref("stg_airbyte_source__ballotready_api_place") }} as p
    on p.database_id = f.database_id
where p.mtfcc = 'G4110' and f.county_fips is null
having count(*) > 300
