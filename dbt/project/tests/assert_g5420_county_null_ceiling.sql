-- DATA-1950 (G5420): NULL county rises BY DESIGN (~859 mart / ~3,183 intermediate
-- today).
-- This caps a strip regression that over-nulls; it is NOT a "stays near today" guard.
select count(*) as g5420_null_county
from {{ ref("int__place_fast_facts") }} as f
join
    {{ ref("stg_airbyte_source__ballotready_api_place") }} as p
    on p.database_id = f.database_id
where p.mtfcc = 'G5420' and f.county_fips is null
having count(*) > 3600
