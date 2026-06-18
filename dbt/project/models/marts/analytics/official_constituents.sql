{{ config(materialized="view", tags=["marts", "analytics", "serve"]) }}

-- official_constituents (DATA-1993): the Serve per-official display surface -- one row
-- per resolved serve official with the population of the jurisdiction they serve
-- ("your district has N constituents, M registered voters"). A clean read off
-- district_census_stats; unaffected by the count-once dedup. Local officials join on
-- (state, type, normalized name); statewide officials (is_statewide) read the
-- district_type='State' row by state (T5 join contract). Each resolver row uses exactly
-- one branch (the is_statewide guard), and district_census_stats is unique on its key
-- with one 'State' row per state, so there is no fan-out: one row per official.
--
-- Officials whose district is a deferred special-district type or whose name drifted
-- bind
-- nothing -> has_census_match=false, null constituents (a documented v1-scope /
-- re-match
-- gap, not a defect). requires_review is carried through so the product can caveat the
-- known wrong-scope matches (pending the DATA-1989 upstream overrides); flagged rows
-- are
-- NEVER filtered here. The high-undercount flag is deferred to a follow-up (a correct
-- voterless/invisible-population measure needs geographic assignment of zero-voter
-- blocks,
-- out of v1 scope); v1 ships constituents_minus_voters, the honest raw gap. View
-- materialization (a thin always-fresh lookup, like the sibling users_serve_activity).
with
    resolver as (
        select *
        from {{ ref("int__serve_district_resolution") }}
        where resolution_path != 'unresolved'
    ),

    stats as (select * from {{ ref("district_census_stats") }})

select
    r.organization_slug,
    r.user_id,
    r.state,
    r.l2_district_type,
    r.normalized_district_name,
    r.is_statewide,
    r.resolution_path,
    r.requires_review,
    r.is_geo_seat,
    coalesce(s.district_population, sw.district_population) as constituents,
    coalesce(s.registered_voters, sw.registered_voters) as registered_voters,
    coalesce(s.district_population, sw.district_population)
    - coalesce(s.registered_voters, sw.registered_voters) as constituents_minus_voters,
    (
        s.state_postal_code is not null or sw.state_postal_code is not null
    ) as has_census_match
from resolver r
left join
    stats s
    on not r.is_statewide
    and r.state = s.state_postal_code
    and r.l2_district_type = s.district_type
    and r.normalized_district_name = s.district_name
    and s.district_type != 'State'
left join
    stats sw
    on r.is_statewide
    and r.state = sw.state_postal_code
    and sw.district_type = 'State'
