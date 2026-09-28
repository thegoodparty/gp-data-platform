-- Every jurisdiction the seed clears must actually mint districts.
--
-- The only guard here that fails on absence rather than presence. Every other
-- check fires when something wrong is in the data, so a vendor rename that
-- emptied the proposed column, or a forgotten full refresh, would leave the whole
-- suite green with the feature silently gone.
--
-- It is also the year-end detector the retirement plan depends on. When L2 folds
-- the new map into the current typed column and clears the proposed one, this goes
-- red - which is expected, and is the signal to remove the override rows before
-- the mint stops producing. Removing them afterwards nulls the district on every
-- routed position rather than reverting it.
with
    minted_scopes as (
        select distinct
            state_postal_code as state, shadowed_district_type as district_type
        from {{ ref("int__l2_proposed_district_aggregations") }}
    )

select adoption.state, adoption.district_type
from {{ ref("district_map_adoption") }} as adoption
left join
    minted_scopes
    on minted_scopes.state = adoption.state
    and minted_scopes.district_type = adoption.district_type
where
    adoption.adopted_source = 'proposed'
    and adoption.is_verified
    and minted_scopes.state is null
