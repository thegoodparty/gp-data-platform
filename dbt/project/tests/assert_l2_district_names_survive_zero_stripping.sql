-- strip_l2_district_zero_padding would empty an all-zero name and detach its voters.
select district_type, district_name
from {{ ref("int__l2_district_aggregations") }}
where district_name rlike '^0+$'
