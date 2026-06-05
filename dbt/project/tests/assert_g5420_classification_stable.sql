-- DATA-1950 (G5420): every staging G5420 place appears in the model exactly once
-- (anti-join per database_id so a missing row cannot be masked by a duplicate).
with
    staging_g5420 as (
        select database_id
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        where mtfcc = 'G5420'
    )
select s.database_id, count(f.database_id) as n_in_model
from staging_g5420 as s
left join {{ ref("int__place_fast_facts") }} as f on f.database_id = s.database_id
group by s.database_id
having count(f.database_id) <> 1
