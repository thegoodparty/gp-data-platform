{{ config(severity="error") }}

-- Independent level-presence wall with LITERAL predicates, deliberately NOT
-- using the shared census_acs_retained_geo_rows macro: if an edit to that
-- macro ever dropped or added a summary level, the staging model and every
-- macro-based test would drift together invisibly. This test cannot drift
-- with them. Staging must contain rows for each of exactly these eight
-- summary levels and no other.
with
    staged as (
        select summary_level, count(*) as row_count
        from {{ ref("stg_census_acs__geo_estimates") }}
        group by summary_level
    ),

    expected as (
        select '150' as summary_level
        union all
        select '040'
        union all
        select '050'
        union all
        select '160'
        union all
        select '010'
        union all
        select '950'
        union all
        select '960'
        union all
        select '970'
    )

select 'missing_level' as violation, expected.summary_level as detail
from expected
left anti join staged using (summary_level)
union all
select 'unexpected_level' as violation, staged.summary_level as detail
from staged
left anti join expected using (summary_level)
