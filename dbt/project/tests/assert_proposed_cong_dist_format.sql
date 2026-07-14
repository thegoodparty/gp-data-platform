-- Fail loudly if a proposed congressional value drifts from the
-- "<year> PROPOSED CONG DIST <n>" format that district resolution parses.
select state_postal_code, proposed_district
from {{ ref("int__l2_nationwide_uniform") }}
where
    upper(proposed_district) like '%PROPOSED CONG DIST%'
    and not regexp_like(proposed_district, '^[0-9]{4} PROPOSED CONG DIST [0-9]+')
