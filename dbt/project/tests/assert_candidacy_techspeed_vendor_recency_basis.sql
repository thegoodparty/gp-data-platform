-- The sibling of assert_candidacy_hubspot_vendor_recency_basis, for the other feed.
-- A vendor record that was merely re-delivered unchanged must not read as recent
-- activity here either. This feed has its own window predicate and its own vendor
-- joins, so the sibling test does not cover it: a reverted window basis or a dropped
-- vendor join on this model alone would otherwise go unnoticed.
--
-- Asserted against the intermediates' vendor_activity_at rather than the model's own
-- filter, so it also fires if the clamp starts overwriting vendor time with extract
-- time. The intermediates' not_null tests on that column keep it from passing
-- vacuously.
--
-- gp_api rows are excluded because their leg of the recency expression is a real
-- product timestamp, so one can legitimately be in-window with a stale vendor leg.
-- This feed does not emit the source (its column contract is fixed), so the source
-- comes from the candidacy mart.
--
-- 17 rather than 16 days: model and test can straddle a midnight rollover, which
-- moves current_date() forward a day between them. The cohort this guards is months
-- stale, so a day of slack costs nothing.
select f.gp_candidacy_id, f.last_activity_at, br.vendor_activity_at
from {{ ref("candidacy_techspeed") }} as f
inner join
    {{ ref("int__civics_candidacy_ballotready") }} as br
    on f.gp_candidacy_id = br.gp_candidacy_id
inner join {{ ref("candidacy") }} as cy on f.gp_candidacy_id = cy.gp_candidacy_id
left join
    {{ ref("int__civics_candidacy_techspeed") }} as ts
    on f.gp_candidacy_id = ts.gp_candidacy_id
where
    -- coalesce, not a bare !=: candidate_id_source is nullable on the mart, and a
    -- bare comparison would silently drop null-source rows from the guard.
    coalesce(cy.candidate_id_source, '') != 'gp_api'
    and br.vendor_activity_at < current_date() - interval 17 day
    and ts.vendor_activity_at is null
