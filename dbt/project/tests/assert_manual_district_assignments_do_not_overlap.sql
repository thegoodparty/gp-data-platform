-- Two assignment rows of the same district type must not both match one voter.
-- The match tuple is sparse (null means "do not constrain"), so distinct rows
-- can still overlap -- a county-wide row and a precinct row in that county both
-- match the precinct's voters. int__l2_nationwide_uniform
-- left-joins per district type, so an overlap fans the voter file out and
-- inflates every downstream count and export. Rows identical on the full tuple
-- are excluded here; unique_combination_of_columns on the seed covers those.
select
    a.state,
    a.l2_district_type,
    a.l2_district_name as name_a,
    b.l2_district_name as name_b
from {{ ref("l2_manual_district_assignments") }} as a
join
    {{ ref("l2_manual_district_assignments") }} as b
    on a.state = b.state
    and a.l2_district_type = b.l2_district_type
    and (a.county is null or b.county is null or a.county = b.county)
    and (a.city is null or b.city is null or a.city = b.city)
    and (a.precinct is null or b.precinct is null or a.precinct = b.precinct)
where not (a.county <=> b.county and a.city <=> b.city and a.precinct <=> b.precinct)
