-- DATA-1986: every (zip, state) in int__zip_code_to_l2_district must have a
-- synthetic 'State' district row. Statewide positions (Governor, U.S. Senate,
-- etc.) are matched to l2_district_type='State' by the LLM BR Office <-> L2
-- District matcher, so without a (zip, 'State', <state>) bridge row per
-- in-state zip they get zero zip->office coverage and never appear in the
-- zip-scoped officepicker.
--
-- This guards the bridge rows this change adds, independent of which dated
-- matcher model the pipeline currently references. Each row returned is a
-- (zip, state) that has district rows but no 'State' row.
with
    zip_states as (
        select distinct zip_code, state_postal_code
        from {{ ref("int__zip_code_to_l2_district") }}
    ),
    state_coverage as (
        select distinct zip_code, state_postal_code
        from {{ ref("int__zip_code_to_l2_district") }}
        where district_type = 'State'
    )
select z.zip_code, z.state_postal_code
from zip_states as z
left join
    state_coverage as s
    on z.zip_code = s.zip_code
    and z.state_postal_code = s.state_postal_code
where s.zip_code is null
