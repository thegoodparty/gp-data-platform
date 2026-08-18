-- Fail loudly if a proposed congressional district name drifts from the
-- "<year> PROPOSED CONG DIST <n>" format district resolution parses.
--
-- Reads the vendor column, not the district dimension. The gate drops a value
-- whose district number will not parse, so a malformed name never reaches the
-- dimension — checking there would hide precisely the drift this exists to
-- catch. This is also the one guard that deliberately does not go through the
-- shared parse macros: it is what makes a bug in those macros detectable.
-- No end anchor on purpose: every value carries a trailing " (EST.)", and the
-- guard mirrors the deliberate prefix match the resolver does.
select distinct l2.state_postal_code, l2.proposed_district
from {{ ref("int__l2_nationwide_uniform") }} as l2
where
    upper(l2.proposed_district) like '%PROPOSED CONG DIST%'
    and not regexp_like(
        upper(l2.proposed_district), '^[0-9]{4} PROPOSED CONG DIST [0-9]+'
    )
