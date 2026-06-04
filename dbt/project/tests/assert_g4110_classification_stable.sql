-- dbt/project/tests/assert_g4110_classification_stable.sql
-- DATA-1950: every staging G4110 place must appear EXACTLY ONCE in the model.
-- Uses an anti-join + per-id count rather than comparing aggregate totals, so a
-- missing G4110 row cannot be masked by a duplicate elsewhere (a count-only test
-- can stay green when one row is dropped and another duplicated). Returns a row
-- for any staging G4110 place that is absent from the model (model_count = 0) or
-- present more than once (model_count > 1).
with
    src_g4110 as (
        select database_id
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        where mtfcc = 'G4110'
    ),
    model_counts as (
        select f.database_id, count(*) as n
        from {{ ref("int__place_fast_facts") }} as f
        join src_g4110 as s on s.database_id = f.database_id
        group by f.database_id
    )
select s.database_id, coalesce(m.n, 0) as model_count
from src_g4110 as s
left join model_counts as m on m.database_id = s.database_id
where m.database_id is null or m.n <> 1
