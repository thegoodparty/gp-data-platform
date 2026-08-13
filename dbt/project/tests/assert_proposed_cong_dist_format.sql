-- Fail loudly if a proposed congressional district name drifts from the
-- "<year> PROPOSED CONG DIST <n>" format district resolution parses. Checked
-- against the district dimension, one row per (state, type, name), rather than
-- the voter-grain L2 view: same coverage, a fraction of the scan.
-- No end anchor on purpose: every value carries a trailing " (EST.)", and the
-- guard mirrors the deliberate prefix match the resolver does.
select state, l2_district_type, l2_district_name
from {{ ref("m_election_api__district") }}
where
    l2_district_type = 'Proposed_District'
    and upper(l2_district_name) like '%PROPOSED CONG DIST%'
    and not regexp_like(upper(l2_district_name), '^[0-9]{4} PROPOSED CONG DIST [0-9]+')
