-- Within one district type, the districts sharing a block group cannot be given
-- more than the whole block group: their allocated population summed over the
-- block group stays at or under its 2020 census population. Exceeding it would
-- double-count ACS counts into the acs_* columns. Returns offending rows.
with
    allocated as (
        select
            block_group_geoid,
            district_type,
            sum(allocated_population) as allocated_population
        from {{ ref("int__district_census_allocation") }}
        group by block_group_geoid, district_type
    ),

    block_group_population as (
        select
            left(block_geoid, 12) as block_group_geoid,
            sum(population) as block_group_population
        from {{ ref("stg_census__block_population") }}
        group by left(block_geoid, 12)
    )

select
    allocated.block_group_geoid,
    allocated.district_type,
    allocated.allocated_population,
    block_group_population.block_group_population
from allocated
inner join
    block_group_population
    on allocated.block_group_geoid = block_group_population.block_group_geoid
where
    allocated.allocated_population
    > block_group_population.block_group_population + 1e-6
