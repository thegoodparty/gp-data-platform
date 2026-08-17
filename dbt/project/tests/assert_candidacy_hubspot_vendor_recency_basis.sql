-- A vendor record that was merely re-delivered unchanged must not read as recent
-- activity. BallotReady began re-sending its entire candidate file weekly, and
-- because the vendor intermediates stamp created_at/updated_at from the pipeline's
-- extract time, each re-delivery re-marked the whole vendor universe as active
-- today and flooded this feed with thousands of long-dormant candidacies. The
-- window now runs on the vendor's own event time, so no row may be in the feed
-- whose only claim to being in-window is the extract stamp.
--
-- This is not a restatement of the model's WHERE clause: it asserts against the
-- intermediates' vendor_activity_at, so it also fires if a vendor join is dropped,
-- if the clamp starts overwriting vendor time with extract time, or if the window
-- basis is reverted while the emitted column is left alone. The intermediates'
-- not_null tests on vendor_activity_at keep it from passing vacuously.
--
-- gp_api rows are excluded because their leg of the recency expression is a real
-- product timestamp, so one can legitimately be in-window with a stale vendor leg.
-- DDHQ-only rows have no vendor leg at all and keep status-quo behavior; the inner
-- join below already leaves them out.
--
-- 31 rather than 30 days: model and test can straddle a midnight rollover, which
-- moves current_date() forward a day between them. The cohort this guards is
-- months stale, so a day of slack costs nothing.
select f.gp_candidacy_id, f.last_activity_at, br.vendor_activity_at
from {{ ref("candidacy_hubspot") }} as f
inner join
    {{ ref("int__civics_candidacy_ballotready") }} as br
    on f.gp_candidacy_id = br.gp_candidacy_id
left join
    {{ ref("int__civics_candidacy_techspeed") }} as ts
    on f.gp_candidacy_id = ts.gp_candidacy_id
where
    f.`Candidate ID Source` != 'gp_api'
    and br.vendor_activity_at < current_date() - interval 31 day
    and ts.vendor_activity_at is null
