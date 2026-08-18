-- Every state seeded as adopted must actually produce proposed districts.
--
-- The other guards here are all negative: they fail when something wrong is
-- present. That leaves a whole class of failure silent — if the vendor renames
-- the value (say "PROPOSED CONGRESSIONAL DISTRICT" instead of "CONG DIST"), the
-- token match stops matching, the gate drops every proposed value, and each
-- negative guard passes because there is nothing left to object to. The feature
-- disappears with a green suite.
--
-- The same hole covers a forgotten full refresh: the district mart is
-- incremental on loaded_at, so a run whose watermark is ahead of the source
-- mints nothing at all, silently.
--
-- So assert the positive: a state we have decided to route must have somewhere
-- to route to.
with
    adopted_states as (
        select distinct state
        from {{ ref("district_map_adoption") }}
        where
            district_type = 'US_Congressional_District'
            and adopted_source = 'proposed'
            and is_verified
    ),
    minted as (
        select state, count(*) as districts
        from {{ ref("m_election_api__district") }}
        where l2_district_type = 'Proposed_District'
        group by state
    )
select adopted_states.state, coalesce(minted.districts, 0) as districts
from adopted_states
left join minted on minted.state = adopted_states.state
where coalesce(minted.districts, 0) = 0
