-- A district that L2 says holds voters must have DistrictVoter rows.
--
-- Thresholded rather than all-or-nothing, because a handful of unlinked
-- districts is routine and a wave of them is the incident this exists to catch.
--
-- The two sides are read at different delivery vintages. registered_voters comes
-- from int__l2_district_aggregations over int__l2_nationwide_uniform; the
-- DistrictVoter side comes from m_people_api__voter over
-- int__l2_nationwide_uniform_w_haystaq, whose merge lags. Measured 2026-08-23:
-- Colorado's uniform watermark was 2026-08-22 against a haystaq watermark of
-- 2026-07-29, so 23 districts the August delivery created had counts but no
-- links yet. Those resolve on the next haystaq merge. As a bare error this
-- skipped m_people_api__districtstats on every merge build in that window.
--
-- The band is wide because the two cases are two orders of magnitude apart: 25
-- of 125,603 districts in scope on that date, against the ~2,500 that reached
-- serving reporting zero constituents. 250 sits ten times above routine churn
-- and ten times below the systemic break, so a real regression still fails the
-- build while a delivery landing ahead of the haystaq merge does not.
--
-- Two things it catches that nothing else does, since every other test checks
-- the opposite direction (a DistrictVoter row pointing at a real district and
-- voter): a live district that has genuinely lost its links, and a retired
-- district carrying a stale count - the aggregation is append-only, which is how
-- BARROW CITY (EST.) still reports voters after the rename to Utqiagvik.
--
-- Do not narrow this by comparing the vintages here. That needs a watermark from
-- a monthly-tagged model, CI excludes that tag, and dbt drops any test whose
-- parents are excluded - so the test would stop running in CI entirely.
{{ config(severity="error", error_if=">250", warn_if=">0") }}
--
-- The existing tests only check the other direction: that a DistrictVoter row
-- points at a real district and a real voter. Nothing caught a district losing
-- its whole voter set, which is how ~2,500 districts reached serving reporting
-- zero constituents. The exact-match join returns nothing rather than erroring,
-- so the failure stayed silent all the way to the customer.
--
-- This only holds once the Voter and DistrictVoter marts have rebuilt with the
-- seven district columns the Voter mart was missing. Against a DistrictVoter
-- built before that, it reports exactly those types' districts.
--
-- Scoped to the district types currently collected from L2. 'State' is excluded
-- because DistrictStats derives statewide associations straight from
-- Voter.state, so those synthetic rows carry no DistrictVoter links. Types
-- absent from the list are excluded because the aggregation is append-only and
-- keeps rows for types since dropped from it; those have no voter column left to
-- link through, and pruning them is a separate job.
select district.id, district.state, district.l2_district_type, district.l2_district_name
from {{ ref("m_election_api__district") }} as district
left join
    (
        select distinct district_id from {{ ref("m_people_api__districtvoter") }}
    ) as linked
    on district.id = linked.district_id
where
    district.registered_voters > 0
    and linked.district_id is null
    and district.l2_district_type in (
        {%- for district_type in get_l2_district_types() %}
            '{{ district_type }}'{{ "," if not loop.last }}
        {%- endfor %}
    )
