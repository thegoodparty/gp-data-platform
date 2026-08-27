-- Fail loudly if a handled proposed district name drifts from the
-- "<year> PROPOSED <TYPE> DIST <n>" shape any parse of this column depends on.
-- Covers both handled types: congressional, and MI's state senate.
--
-- Reads the vendor column directly, and deliberately does not go through the
-- shared parse macro: that is what keeps a bug in the macro detectable.
--
-- Anchored at both ends, with the trailing " (EST.)" congressional values carry
-- spelled out as optional. Without the end anchor, "... DIST 5 INVALID EXTRA"
-- satisfies the pattern and reaches district resolution unflagged; anchoring
-- without allowing the suffix rejects every congressional value instead, since
-- only MI's state senate values arrive bare (verified: 7,958,045 of 101,966,221).
--
-- Strict on leading whitespace where the parse macros are tolerant, and that
-- asymmetry is deliberate. The macros are unanchored, so a vendor formatting
-- change keeps routing working while this guard still reports it.
select distinct l2.state_postal_code, l2.proposed_district
from {{ ref("int__l2_nationwide_uniform") }} as l2
where
    regexp_like(upper(l2.proposed_district), 'PROPOSED (CONG|STATE SEN) DIST')
    and not regexp_like(
        upper(l2.proposed_district),
        '^[0-9]{4} PROPOSED (CONG|STATE SEN) DIST [0-9]+( \\(EST\\.\\))?$'
    )
