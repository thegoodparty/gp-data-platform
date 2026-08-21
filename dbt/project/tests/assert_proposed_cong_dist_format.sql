-- Fail loudly if a proposed congressional district name drifts from the
-- "<year> PROPOSED CONG DIST <n>" shape any parse of this column depends on.
--
-- Reads the vendor column directly, and deliberately does not go through the
-- shared parse macro: that is what keeps a bug in the macro detectable.
-- No end anchor on purpose, since every value carries a trailing " (EST.)".
select distinct l2.state_postal_code, l2.proposed_district
from {{ ref("int__l2_nationwide_uniform") }} as l2
where
    upper(l2.proposed_district) like '%PROPOSED CONG DIST%'
    and not regexp_like(
        upper(l2.proposed_district), '^[0-9]{4} PROPOSED CONG DIST [0-9]+'
    )
