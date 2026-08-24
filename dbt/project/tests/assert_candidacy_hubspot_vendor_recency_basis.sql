-- Wherever a vendor supplies an event time, the feed's emitted recency MUST be that
-- event time -- never the pipeline's extract stamp. That is the incident this fix
-- exists to prevent: the vendor began re-sending its whole candidate file weekly, and
-- because the intermediates stamp created_at/updated_at from extract time, each
-- re-delivery re-marked the entire vendor universe as active today and flooded this
-- feed with long-dormant candidacies.
--
-- Stated as an identity rather than a staleness cutoff, which buys three things:
-- it covers every row with a vendor leg instead of only the BallotReady-only ones;
-- it fires if the window basis is reverted to the canonical timestamps, or if either
-- vendor join is dropped, because both make the emitted value fall back to extract
-- time; and it has no date arithmetic, so it cannot false-fail when tests run a day
-- or more after the table was built.
--
-- greatest() skips nulls on Spark, so rows where no provider supplied an event time
-- yield null and the inequality below filters them out. Those keep status-quo
-- behavior by design and are not asserted here.
--
-- gp_api rows are excluded because their leg IS the canonical product timestamps, so
-- for them the emitted value legitimately differs from the vendor legs. The source is
-- read from candidacy_scored, the same relation the model gates on, rather than from
-- the feed's Title-Case import-contract column, which ops owns and may relabel.
select
    f.gp_candidacy_id,
    f.last_activity_at,
    greatest(br.vendor_activity_at, ts.vendor_activity_at) as vendor_basis
from {{ ref("candidacy_hubspot") }} as f
inner join {{ ref("candidacy_scored") }} as cy on f.gp_candidacy_id = cy.gp_candidacy_id
left join
    {{ ref("int__civics_candidacy_ballotready") }} as br
    on f.gp_candidacy_id = br.gp_candidacy_id
left join
    {{ ref("int__civics_candidacy_techspeed") }} as ts
    on f.gp_candidacy_id = ts.gp_candidacy_id
where
    coalesce(cy.candidate_id_source, '') != 'gp_api'
    and f.last_activity_at != greatest(br.vendor_activity_at, ts.vendor_activity_at)
