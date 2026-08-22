-- Fail loudly if a handled proposed district name drifts from the
-- "<year> PROPOSED <TYPE> DIST <n>" shape any parse of this column depends on.
-- Covers both handled types: congressional, and MI's state senate.
--
-- Reads the vendor column directly, and deliberately does not go through the
-- shared parse macro: that is what keeps a bug in the macro detectable.
-- No end anchor on purpose, since congressional values carry a trailing
-- " (EST.)" that state senate values do not.
select distinct l2.state_postal_code, l2.proposed_district
from {{ ref("int__l2_nationwide_uniform") }} as l2
where
    regexp_like(upper(l2.proposed_district), 'PROPOSED (CONG|STATE SEN) DIST')
    and not regexp_like(
        upper(l2.proposed_district), '^[0-9]{4} PROPOSED (CONG|STATE SEN) DIST [0-9]+'
    )
