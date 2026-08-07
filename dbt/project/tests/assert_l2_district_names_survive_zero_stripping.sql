-- ltrim('0', ...) empties an all-zero name, detaching its voters. This reads the
-- stripped value, so the surviving evidence is the empty string, not '^0+$'.
select district_type, district_name
from {{ ref("int__l2_district_aggregations") }}
where district_name = ''
