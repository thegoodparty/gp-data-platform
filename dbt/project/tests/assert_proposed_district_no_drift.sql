-- Proposed_District is a catch-all for district changes, so a value shape we
-- have never seen is expected rather than exceptional. Fail on anything that is
-- neither one of the two handled shapes nor an explicitly ignored value, so a
-- new shape surfaces as a question for the vendor instead of falling through
-- unnoticed.
--
-- Load-bearing rather than defensive. The 99.6% of voters on the handled shapes
-- are congressional and MI state senate; the rest are county-commissioner rows
-- and local annexation records that are not seats at all. A parser that quietly
-- swallowed those would mint districts nobody runs in.
select distinct l2.state_postal_code, l2.proposed_district
from {{ ref("int__l2_nationwide_uniform") }} as l2
left join
    {{ ref("proposed_district_ignored_values") }} as ignored
    on ignored.proposed_district = l2.proposed_district
where
    l2.proposed_district is not null
    and not {{ is_proposed_handled_district("l2.proposed_district") }}
    and ignored.proposed_district is null
