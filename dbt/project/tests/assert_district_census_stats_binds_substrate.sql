-- Binding contract (DATA-1994): every NON-statewide district_census_stats row must
-- equal the direct substrate rollup -- population within float tolerance; voters and
-- distinct-block count exactly -- AND the non-statewide row-set must match the
-- substrate's distinct districts (full outer catches either side missing). Statewide
-- (district_type='State') rows are census-derived, not from the substrate, so they
-- are excluded. A structural drift guard (the mart and this query are the same
-- rollup); conceptual correctness is verified by the Claude reconciliation. Returns
-- offending rows; empty = pass.
with
    substrate_rollup as (
        select
            state_postal_code,
            district_type,
            district_name,
            sum(allocated_population) as substrate_pop,
            sum(voters_in_block_district) as substrate_voters,
            count(distinct block_geoid) as substrate_blocks
        from {{ ref("int__district_census_allocation") }}
        group by state_postal_code, district_type, district_name
    ),

    mart as (
        select
            state_postal_code,
            district_type,
            district_name,
            district_population,
            registered_voters,
            n_census_blocks
        from {{ ref("district_census_stats") }}
        where district_type <> 'State'
    )

select
    coalesce(m.state_postal_code, s.state_postal_code) as state_postal_code,
    coalesce(m.district_type, s.district_type) as district_type,
    coalesce(m.district_name, s.district_name) as district_name
from mart m
full outer join
    substrate_rollup s
    on m.state_postal_code = s.state_postal_code
    and m.district_type = s.district_type
    and m.district_name = s.district_name
where
    m.state_postal_code is null
    or s.state_postal_code is null
    or abs(m.district_population - s.substrate_pop) > 1e-6
    or m.registered_voters <> s.substrate_voters
    or m.n_census_blocks <> s.substrate_blocks
