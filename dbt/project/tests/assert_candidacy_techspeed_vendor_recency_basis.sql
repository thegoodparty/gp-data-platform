-- The sibling of assert_candidacy_hubspot_vendor_recency_basis, for the other feed:
-- wherever a vendor supplies an event time, this feed's emitted recency MUST be that
-- event time and never the pipeline's extract stamp. This model has its own window
-- predicate and its own vendor join, so the sibling test does not cover it.
--
-- Only the BallotReady leg is asserted because it is the only vendor leg this feed
-- has: the ALREADY-SENT filter keeps candidacies with no 'techspeed' in
-- source_systems, and the candidacy mart sets that flag exactly when a row exists in
-- the TechSpeed intermediate, so a TechSpeed leg here can never be populated. When
-- that filter becomes the sent_log anti-join, add the leg to both the model and here.
--
-- Stated as an identity rather than a staleness cutoff, so it covers every row with a
-- vendor leg, fires if the window basis is reverted or the vendor join dropped, and
-- carries no date arithmetic that could false-fail when tests run after the build day.
--
-- gp_api rows are excluded because their leg IS the canonical product timestamps. The
-- source is read from the candidacy mart, the relation this model gates on; this
-- feed's own output cannot carry it, because its 42-column shape is a fixed contract.
select f.gp_candidacy_id, f.last_activity_at, br.vendor_activity_at
from {{ ref("candidacy_techspeed") }} as f
inner join {{ ref("candidacy") }} as cy on f.gp_candidacy_id = cy.gp_candidacy_id
inner join
    {{ ref("int__civics_candidacy_ballotready") }} as br
    on f.gp_candidacy_id = br.gp_candidacy_id
where
    coalesce(cy.candidate_id_source, '') != 'gp_api'
    and f.last_activity_at != br.vendor_activity_at
