-- dbt/project/tests/assert_g4110_classification_stable.sql
-- DATA-1950: every staging G4110 place appears exactly once in the model.
with
    model_g4110 as (
        select count(*) as c
        from {{ ref("int__place_fast_facts") }} as f
        join
            {{ ref("stg_airbyte_source__ballotready_api_place") }} as p
            on p.database_id = f.database_id
        where p.mtfcc = 'G4110'
    ),
    src_g4110 as (
        select count(*) as c
        from {{ ref("stg_airbyte_source__ballotready_api_place") }}
        where mtfcc = 'G4110'
    )
select model_g4110.c as model_count, src_g4110.c as src_count
from model_g4110, src_g4110
where model_g4110.c <> src_g4110.c
