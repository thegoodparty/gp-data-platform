-- Curated nullouts must produce no fallback row, by the seed's own tested key.
-- A candidacy whose gp_election_id is on the nullout seed must be absent from
-- BOTH fallback tiers. Asserted on gp_election_id, not the model's position +
-- date proxy, so the test cannot pass for the same reason the model would fail:
-- seeded rows whose candidacy carries a different position id or election date
-- slip past a position + date key alone.
select candidacy.gp_candidacy_id
from {{ ref("candidacy") }} as candidacy
inner join
    {{ ref("seed_civics_election_2025_position_nullouts") }} as nullouts
    on candidacy.gp_election_id = nullouts.gp_election_id
inner join
    {{ ref("int__civics_viability_seats_fallback") }} as fallback
    on candidacy.gp_candidacy_id = fallback.gp_candidacy_id
