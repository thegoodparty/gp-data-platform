-- dbt/project/tests/assert_g4110_same_name_collision_ceiling.sql
-- DATA-1950: (state, normalized city_ascii) groups spanning >1 county should stay
-- near the measured ~226; a spike means normalization merged distinct names.
with
    collisions as (
        select
            state_id,
            lower(
                trim(
                    regexp_replace(
                        regexp_replace(city_ascii, ' *\\([^)]*\\) *$', ''), ' +', ' '
                    )
                )
            ) as ncity,
            count(distinct county_fips) as nc
        from {{ ref("stg_airbyte_source__ballotready_s3_uscities_v1_77") }}
        group by 1, 2
        having count(distinct county_fips) > 1
    )
select count(*) as colliding_groups
from collisions
having count(*) > 260
